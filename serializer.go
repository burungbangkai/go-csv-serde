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

type SerializationError struct {
}

func (se SerializationError) Error() string {
	return ""
}

type ser struct {
	opts  SerializerOptions
	infos []serinfo
	typ   reflect.Type
}

type Result func() ([]string, error)
type Stop func() error

type Serializer interface {
	Start(ctx context.Context, useHeader bool) (chan<- interface{}, <-chan Result, Stop)
}

type SerializerOptions struct {
	delimiter      rune
	quote          rune
	tagName        string
	bufferSize     int
	timeSerializer vserializer
}

type SerializerOption func(*SerializerOptions)

func WithDelimiter(delimiter rune) SerializerOption {
	return func(opts *SerializerOptions) {
		opts.delimiter = delimiter
	}
}

func WithQuote(quote rune) SerializerOption {
	return func(opts *SerializerOptions) {
		opts.quote = quote
	}
}

func WithTagName(tag string) SerializerOption {
	return func(opts *SerializerOptions) {
		opts.tagName = tag
	}
}

func WithBufferSize(size int) SerializerOption {
	return func(opts *SerializerOptions) {
		if size > 0 {
			opts.bufferSize = size
		}
	}
}

func NewSerializer(theType interface{}, opts ...SerializerOption) (Serializer, error) {
	s := &ser{}
	sops := SerializerOptions{
		quote:      '"',
		tagName:    "csv",
		bufferSize: 10,
	}
	for _, opt := range opts {
		opt(&sops)
	}
	s.opts = sops
	if err := validateInterfaceInput(theType); err != nil {
		return nil, err
	}
	s.typ = reflect.TypeOf(theType)
	s.infos = populateFields(theType, sops.tagName, sops)
	return s, nil
}

type vserializer func(reflect.Value) string

func (s *ser) Start(ctx context.Context, useHeader bool) (chan<- interface{}, <-chan Result, Stop) {
	ch := make(chan interface{}, s.opts.bufferSize)
	res := make(chan Result, s.opts.bufferSize)
	infos := s.infos
	go func() {
		for {
			select {
			case <-ctx.Done():
				res <- func() ([]string, error) {
					return nil, ctx.Err()
				}
				close(res)
				return
			case val, chOpen := <-ch:
				if !chOpen {
					close(res)
					return
				}
				if useHeader {
					useHeader = false
					res <- func() ([]string, error) {
						header := make([]string, 0, len(infos))
						for _, info := range infos {
							header = append(header, info.Name)
						}
						return header, nil
					}
				}
				valTyp := reflect.TypeOf(val)
				if valTyp != s.typ {
					res <- func() ([]string, error) {
						return nil, fmt.Errorf("value type is not %T", s.typ)
					}
					continue
				}
				res <- func() ([]string, error) {
					values := make([]string, 0, len(infos))
					v := reflect.ValueOf(val)
					for _, info := range infos {
						fv := getByFieldIndex(v, info.FieldIndex)
						values = append(values, info.Fn(fv))
					}
					return values, nil
				}
			}
		}
	}()
	once := &sync.Once{}
	stop := func() error {
		once.Do(func() {
			close(ch)
		})
		return nil
	}
	return ch, res, stop
}

type serinfo struct {
	Name       string
	FieldIndex []int
	Fn         vserializer
}

// we use this
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

func getValueSerializer(typ reflect.Type, val reflect.Value, opts SerializerOptions) vserializer {
	if _, ok := val.Interface().(encoding.TextMarshaler); ok {
		return defaultVTextMarshaller
	}
	var vser vserializer
	switch typ.Kind() {
	case reflect.Interface, reflect.Func, reflect.Chan:
		return nil
	case reflect.Ptr:
		typ = typ.Elem()
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
		ivser := getValueSerializer(typ.Elem(), val, opts)
		vser = createSliceVseralizer(ivser, opts.quote)
	default:
		vser = defaultVserializer
	}
	return vser
}

func populateFields(any interface{}, tagName string, opts SerializerOptions) []serinfo {
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
		if tagName != "" {
			str := f.Tag.Get(tagName)
			if str == "" || str == "-" {
				continue
			}
			name = str
		}
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
		info := serinfo{
			Name:       name,
			Fn:         vser,
			FieldIndex: f.Index,
		}
		infos = append(infos, info)
		if fval.Kind() == reflect.Struct {
			if _, ok := fval.Interface().(encoding.TextMarshaler); ok {
				continue
			}
			infos = infos[:len(infos)-1]
			iinfos := populateFields(fval.Interface(), tagName, opts)
			for _, iinfo := range iinfos {
				iinfo.Name = name + "." + iinfo.Name
				iinfo.FieldIndex = append(f.Index, iinfo.FieldIndex...)
				infos = append(infos, iinfo)
			}
		}

	}
	return infos
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

func createSliceVseralizer(elemvser vserializer, quote rune) vserializer {
	return func(v reflect.Value) string {
		for v.Kind() == reflect.Ptr {
			if v.IsNil() {
				empty := []rune{quote, quote}
				return string(empty)
			}
			v = v.Elem()
		}
		if !v.IsValid() {
			empty := []rune{quote, quote}
			return string(empty)
		}
		sb := strings.Builder{}
		sb.WriteRune(quote)
		for i := 0; i < v.Len(); i++ {
			sb.WriteString(elemvser(v.Index(i)))
			if i < v.Len()-1 {
				sb.WriteString(",")
			}
		}
		sb.WriteRune(quote)
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
	return fmt.Sprint()
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
