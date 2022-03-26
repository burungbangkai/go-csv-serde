package csvserde

import (
	"context"
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

var strBuilders = sync.Pool{
	New: func() interface{} {
		return &strings.Builder{}
	},
}

type ser struct {
	opts  serializerOptions
	infos []serinfo
	typ   reflect.Type
}

// SerResult is function that returns the result of the serialization
// this is a work around because channel can only have one type
type SerResult func() ([]string, error)
type Stop func() error

type Serializer interface {
	// Start starts the serializer. it will return 2 channel and a stopper function.
	// the first channel is where the client send the input,
	// the second channel is whhere the serializaion result will be sent.
	// when client does not have any other data to push to the input channel,
	// it must call the stopper function to stop the serializer goroutine cleanly and prevent memory leak.
	Start(ctx context.Context, produceHeader bool) (input chan<- interface{}, result <-chan SerResult, stopper Stop)
}

type serializerOptions struct {
	delimiter            rune
	quote                rune
	tagName              string
	bufferSize           int
	sliceOrArrayEncloser [2]rune
}

type SerializerOption func(*serializerOptions)

func WithDelimiter(delimiter rune) SerializerOption {
	return func(opts *serializerOptions) {
		opts.delimiter = delimiter
	}
}

func WithQuote(quote rune) SerializerOption {
	return func(opts *serializerOptions) {
		opts.quote = quote
	}
}

func WithTagName(tag string) SerializerOption {
	return func(opts *serializerOptions) {
		opts.tagName = tag
	}
}

func WithBufferSize(size int) SerializerOption {
	return func(opts *serializerOptions) {
		if size > 0 {
			opts.bufferSize = size
		}
	}
}

func WithSliceOrArrayEncloser(encloser [2]rune) SerializerOption {
	return func(opts *serializerOptions) {
		opts.sliceOrArrayEncloser = encloser
	}
}

func NewSerializer(theType interface{}, opts ...SerializerOption) (Serializer, error) {
	s := &ser{}
	sops := serializerOptions{
		tagName:              "csv",
		bufferSize:           10,
		delimiter:            ',',
		sliceOrArrayEncloser: [2]rune{'[', ']'},
	}
	for _, opt := range opts {
		opt(&sops)
	}
	s.opts = sops
	if err := validateInterfaceInput(theType); err != nil {
		return nil, err
	}
	s.typ = reflect.TypeOf(theType)
	s.infos = populateFields(theType, sops)
	return s, nil
}

type vserializer func(reflect.Value) string

func (s *ser) Start(ctx context.Context, produceHeader bool) (chan<- interface{}, <-chan SerResult, Stop) {
	ch := make(chan interface{}, s.opts.bufferSize)
	res := make(chan SerResult, s.opts.bufferSize)
	infos := s.infos
	once := &sync.Once{}
	stop := func() error {
		once.Do(func() {
			close(ch)
		})
		return nil
	}
	go func() {
		headerOnce := &sync.Once{}
		for {
			select {
			case <-ctx.Done():
				res <- func() ([]string, error) {
					return nil, ctx.Err()
				}
				_ = stop()
				return
			case val, chOpen := <-ch:
				if !chOpen {
					close(res)
					return
				}
				headerOnce.Do(func() {
					if produceHeader {
						res <- func() ([]string, error) {
							header := make([]string, 0, len(infos))
							for _, info := range infos {
								header = append(header, info.Name)
							}
							return header, nil
						}
					}
				})

				valTyp := reflect.TypeOf(val)
				if valTyp != s.typ {
					res <- func() ([]string, error) {
						return nil, fmt.Errorf("value type is not %T", s.typ)
					}
					continue
				}
				res <- func() ([]string, error) {
					values := make([]string, len(infos))
					v := reflect.ValueOf(val)
					for idx, info := range infos {
						fv := getByFieldIndex(v, info.FieldIndex)
						values[idx] = info.Fn(fv)
					}
					return values, nil
				}
			}
		}
	}()
	return ch, res, stop
}

type serinfo struct {
	Name       string
	FieldIndex []int
	Fn         vserializer
}

// we use this because reflect.Value.FieldByIndex() will panic if the underlying value is nil
func getByFieldIndex(v reflect.Value, index []int) reflect.Value {
	for _, x := range index {
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		if !v.IsValid() {
			return v
		}
		v = v.Field(x)
	}
	return v
}

func validateInterfaceInput(inf interface{}) error {
	typ := reflect.TypeOf(inf)
	if typ == nil {
		return errors.New("can not use nil")
	}
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return errors.New("can only use struct")
	}
	return nil
}

func getValueSerializer(typ reflect.Type, val reflect.Value, opts serializerOptions) vserializer {
	if _, ok := val.Interface().(encoding.TextMarshaler); ok {
		return defaultVTextMarshaller
	}
	var vser vserializer
	switch typ.Kind() {
	case reflect.Interface, reflect.Func, reflect.Chan:
		return nil
	case reflect.Ptr:
		typ = typ.Elem()
		val = reflect.New(typ)
		vser = getValueSerializer(typ, val, opts)
	case reflect.Bool:
		vser = boolVserializer
	case reflect.Int64:
		if typ.PkgPath() == "time" && typ.Name() == "Duration" {
			vser = durationVseralizer
			break
		}
		fallthrough
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		vser = intVserializer
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		vser = uintVserializer
	case reflect.Float32, reflect.Float64:
		vser = createFloatVserialzer(typ.Bits())
	case reflect.String:
		vser = stringVseralizer
	case reflect.Slice, reflect.Array:
		typ = typ.Elem()
		val = reflect.New(typ)
		evser := getValueSerializer(typ, val, opts)
		vser = createSliceVseralizer(evser, opts)
	case reflect.Struct:
		vser = createStructVseralizer(typ, opts)
	default:
		vser = defaultVserializer
	}
	return vser
}

func populateFields(any interface{}, opts serializerOptions) []serinfo {
	typ := reflect.TypeOf(any)
	val := reflect.ValueOf(any)
	infos := make([]serinfo, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		fval := val.Field(i)
		ftyp := f.Type
		// the field is not public skip it
		if f.PkgPath != "" {
			continue
		}
		name := f.Name
		if opts.tagName != "" {
			str := f.Tag.Get(opts.tagName)
			if str == "" || str == "-" {
				continue
			}
			name = str
		}
		// iterate until we get the thing that the pointer points to
		for fval.Kind() == reflect.Ptr {
			if fval.IsNil() {
				if fval.Type().Elem().Kind() != reflect.Struct {
					break
				}
				fval = reflect.New(fval.Type().Elem())
			}
			fval = fval.Elem()
			ftyp = fval.Type()
		}
		vser := getValueSerializer(ftyp, fval, opts)
		if vser == nil {
			continue
		}
		// using string build is much faster than using regular string concatenation
		sb := strBuilders.Get().(*strings.Builder)
		sb.WriteRune(opts.quote)
		sb.WriteString(name)
		sb.WriteRune(opts.quote)
		info := serinfo{
			Name:       sb.String(),
			Fn:         vser,
			FieldIndex: f.Index,
		}
		sb.Reset()
		strBuilders.Put(sb)
		infos = append(infos, info)
		if fval.Kind() == reflect.Struct {
			if _, ok := fval.Interface().(encoding.TextMarshaler); ok {
				continue
			}
			infos = infos[:len(infos)-1]
			iinfos := populateFields(fval.Interface(), opts)
			for _, iinfo := range iinfos {
				sb = strBuilders.Get().(*strings.Builder)
				sb.WriteRune(opts.quote)
				iinfo.Name = strings.ReplaceAll(iinfo.Name, string(opts.quote), "")
				sb.WriteString(name)
				sb.WriteRune('.')
				sb.WriteString(iinfo.Name)
				sb.WriteRune(opts.quote)
				iinfo.Name = sb.String()
				sb.Reset()
				strBuilders.Put(sb)
				iinfo.FieldIndex = append(f.Index, iinfo.FieldIndex...)
				infos = append(infos, iinfo)
			}
		}

	}
	return infos
}

func createStructVseralizer(typ reflect.Type, opts serializerOptions) vserializer {
	inf := reflect.New(typ).Elem().Interface()
	infos := populateFields(inf, opts)
	return func(v reflect.Value) string {
		sb := strBuilders.Get().(*strings.Builder)
		defer func() {
			sb.Reset()
			strBuilders.Put(sb)
		}()
		sb.WriteRune(opts.quote)
		for _, info := range infos {
			val := getByFieldIndex(v, info.FieldIndex)
			str := info.Fn(val)
			sb.WriteString(str)
			sb.WriteRune(opts.delimiter)
		}
		sb.WriteRune(opts.quote)
		return sb.String()
	}
}

var defaultVTextMarshaller = func(v reflect.Value) string {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return ""
	}
	tm := v.Interface().(encoding.TextMarshaler)
	b, _ := tm.MarshalText()
	return string(b)
}

func createSliceVseralizer(elemvser vserializer, opts serializerOptions) vserializer {
	return func(v reflect.Value) string {
		for v.Kind() == reflect.Ptr {
			if v.IsNil() {
				empty := []rune{opts.quote, opts.quote}
				return string(empty)
			}
			v = v.Elem()
		}
		if !v.IsValid() {
			empty := []rune{opts.quote, opts.quote}
			return string(empty)
		}
		sb := strBuilders.Get().(*strings.Builder)
		defer func() {
			sb.Reset()
			strBuilders.Put(sb)
		}()
		sb.WriteRune(opts.quote)
		sb.WriteRune('[')
		for i := 0; i < v.Len(); i++ {
			sb.WriteString(elemvser(v.Index(i)))
			if i < v.Len()-1 {
				sb.WriteRune(opts.delimiter)
			}
		}
		sb.WriteRune(']')
		sb.WriteRune(opts.quote)
		return sb.String()
	}
}

func createFloatVserialzer(bitSize int) vserializer {
	return func(v reflect.Value) string {
		for v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return ""
			}
			v = v.Elem()
		}
		if !v.IsValid() {
			return ""
		}
		return strconv.FormatFloat(v.Float(), 'f', -1, bitSize)
	}
}

var defaultVserializer = func(v reflect.Value) string {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return ""
	}
	any := v.Interface()
	if any == nil {
		return ""
	}
	return fmt.Sprintf("%v", any)
}

var boolVserializer = func(v reflect.Value) string {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return ""
	}
	return strconv.FormatBool(v.Bool())
}

var intVserializer = func(v reflect.Value) string {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return ""
	}
	return strconv.FormatInt(v.Int(), 10)
}

var uintVserializer = func(v reflect.Value) string {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return ""
	}
	return strconv.FormatUint(v.Uint(), 10)
}

var stringVseralizer = func(v reflect.Value) string {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return ""
	}
	return v.String()
}

var durationVseralizer = func(v reflect.Value) string {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return ""
	}
	return v.Interface().(time.Duration).String()
}
