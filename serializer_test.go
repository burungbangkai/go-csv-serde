package csvserde

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type T struct {
	IS   []int
	ISP  *[]int
	PIS  []*int
	PF   *F
	PI   *int
	PF32 *float32
	PF64 *float64
	PT2  *T2
	T2   T2
	AI   [3]int
	API  [3]*int
	Any  interface{}
	// T2S  []T2
	// TS   []time.Time
}

type T2 struct {
	T  time.Time
	PT *time.Time
	S  *string
	F  F
}

type F struct {
	S  string
	T  time.Time
	D  time.Duration
	PT *time.Time
	PD *time.Duration
}

func TestPopulateFields(t *testing.T) {
	tt := T{}
	infos := populateFields(tt, serializerOptions{})
	for _, v := range infos {
		t.Logf("name: %s, index: %v, fn: %v", v.Name, v.FieldIndex, v.Fn)
	}
}

func TestCSV(t *testing.T) {
	one, two, three, four, five := 1, 2, 3, 4.0, float32(5.0)
	// now := time.Now()
	// str := "str"
	ts := []T{
		{
			IS:  []int{5, 6, 7},
			ISP: &[]int{1, 2, 3},
			PIS: []*int{&(one), &(two), &(three)},
			PF: &F{
				S: "hello",
				T: time.Now(),
				D: time.Duration(time.Second),
			},
			PI:   &one,
			PF64: &four,
			PF32: &five,
			AI:   [3]int{8, 9, 0},
			API:  [3]*int{&(one), &(two), &(three)},
			Any:  []*int{&(one), &(two), &(three)},
		},
		{
			IS:  []int{5, 6, 7},
			ISP: &[]int{1, 2, 3},
			PIS: []*int{&(one), &(two), &(three)},
			PF: &F{
				S: "hello again",
				T: time.Now().Add(time.Minute),
				D: time.Duration(time.Minute),
			},
		},
		{
			PF: &F{
				S: "hello again again",
				T: time.Now().Add(time.Minute),
				D: time.Duration(time.Minute),
			},
		},
		// {
		// 	T2S: []T2{{time.Now(), nil, &str, F{}}, {time.Now(), &now, nil, F{}}},
		// 	TS:  []time.Time{now, now.Add(1 * time.Hour)},
		// },
	}
	ser, err := NewSerializer(ts[0], WithQuote('-'), WithTagName(""), WithBufferSize(len(ts)/2))
	if err != nil {
		t.Error(err)
	}
	i, r, stop := ser.Start(context.Background(), true)
	go func() {
		i <- ts[0]
		i <- ts[1]
		i <- ts[2]
		// i <- ts[3]
		err := stop()
		if err != nil {
			t.Error(err)
		}
	}()
	for fn := range r {
		str, err := fn()
		if err != nil {
			t.Error(err)
		}
		t.Log(strings.Join(str, ","))
	}
}

var (
	one, two, three, four, five = 1, 2, 3, 4.0, float32(5.0)
	// now                         = time.Now()
	// str                         = "str"
	t1 = T{
		IS:  []int{5, 6, 7},
		ISP: &[]int{1, 2, 3},
		PIS: []*int{&(one), &(two), &(three)},
		PF: &F{
			S: "hello",
			T: time.Now(),
			D: time.Duration(time.Second),
		},
		PI:   &one,
		PF64: &four,
		PF32: &five,
		AI:   [3]int{8, 9, 0},
		API:  [3]*int{&(one), &(two), &(three)},
		Any:  []*int{&(one), &(two), &(three)},

		// T2S: []T2{{time.Now(), nil, &str, F{}}, {time.Now(), &now, nil, F{}}},
		// TS:  []time.Time{now, now.Add(1 * time.Hour)},
	}
	t2 = T{
		IS:  []int{5, 6, 7},
		ISP: &[]int{1, 2, 3},
		PIS: []*int{&(one), &(two), &(three)},
		PF: &F{
			S: "hello again",
			T: time.Now().Add(time.Minute),
			D: time.Duration(time.Minute),
		},
	}
	t3 = T{
		PF: &F{
			S: "hello again again",
			T: time.Now().Add(time.Minute),
			D: time.Duration(time.Minute),
		},
	}
	btable = []struct {
		input []T
	}{}
)

func TestMain(m *testing.M) {
	btable = []struct {
		input []T
	}{
		{
			input: []T{t1},
		},
		{
			input: []T{t1, t2, t3},
		},
		{
			input: []T{
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
			},
		},
		{
			input: []T{
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
				t1, t2, t3, t1, t2, t3, t1, t2, t3, t1, t2, t3,
			},
		},
	}
	os.Exit(m.Run())
}

func BenchmarkCreateCSVSerializer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := NewSerializer(T{}, WithQuote('-'), WithTagName(""), WithBufferSize(100))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCSVReflection(b *testing.B) {
	ser, err := NewSerializer(T{}, WithQuote('-'), WithTagName(""), WithBufferSize(100))
	if err != nil {
		b.Fatal(err)
	}
	for _, binput := range btable {
		b.Run(fmt.Sprintf("input_size_%d", len(binput.input)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				i, r, stop := ser.Start(context.Background(), true)
				go func() {
					for _, input := range binput.input {
						i <- input
					}
					err := stop()
					if err != nil {
						b.Error(err)
					}
				}()
				for fn := range r {
					_, err := fn()
					if err != nil {
						b.Error(err)
					}
				}
			}
		})
	}
}

type handWritten struct {
	opts serializerOptions
}

var _ Serializer = (*handWritten)(nil)

// Start Wrting Wed 16 Mar 2022 22:54:06 WIB
func (s *handWritten) Start(ctx context.Context, useHeader bool) (chan<- interface{}, <-chan SerResult, Stop) {
	ch := make(chan interface{}, s.opts.bufferSize)
	resch := make(chan SerResult, s.opts.bufferSize)
	once := &sync.Once{}
	stop := func() error {
		once.Do(func() {
			close(ch)
		})
		return nil
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				resch <- func() ([]string, error) {
					return nil, ctx.Err()
				}
				_ = stop()
				return
			case val, chOpen := <-ch:
				if !chOpen {
					close(resch)
					return
				}
				if useHeader {
					useHeader = false
					resch <- func() ([]string, error) {
						header := []string{
							"-is-", "-isp-", "-pis-",
							"-pf.s-", "-pf.t-", "-pf.d-", "-pf.pt-", "-pf.pd-",
							"-pi-", "-pf32-", "-pf64-",
							"-pt2.t-", "-pt2.pt-", "-pt2.s-",
							"-t2.t-", "-t2.pt-", "-t2.s-",
							"-ai-", "-api-",
						}
						return header, nil
					}
				}
				t, ok := val.(T)
				if !ok {
					resch <- func() ([]string, error) {
						return nil, fmt.Errorf("value type is not %T", val)
					}
					continue
				}
				res := make([]string, 0, 100)
				is := make([]string, len(t.IS))
				for idx, i := range t.IS {
					str := strconv.FormatInt(int64(i), 10)
					is[idx] = str
				}
				res = append(res, fmt.Sprintf("-[%s]-", strings.Join(is, ",")))
				if t.ISP != nil {
					isp := make([]string, len(*t.ISP))
					for idx, i := range *t.ISP {
						str := strconv.FormatInt(int64(i), 10)
						isp[idx] = str
					}
					res = append(res, fmt.Sprintf("-[%s]-", strings.Join(isp, ",")))
				}
				pis := make([]string, len(t.PIS))
				for idx, i := range t.PIS {
					str := ""
					if i != nil {
						str = strconv.FormatInt(int64(*i), 10)
					}
					pis[idx] = str
				}
				res = append(res, fmt.Sprintf("-[%s]-", strings.Join(pis, ",")))
				if t.PF != nil {
					pf := *t.PF
					res = append(res, pf.S)
					res = append(res, pf.T.String())
					if pf.PT != nil {
						res = append(res, pf.PT.String())
					} else {
						res = append(res, "")
					}
					if pf.PD != nil {
						res = append(res, pf.PD.String())
					} else {
						res = append(res, "")
					}
				} else {
					res = append(res, "")
				}
				if t.PI != nil {
					pi := *t.PI
					res = append(res, strconv.FormatInt(int64(pi), 10))
				} else {
					res = append(res, "")
				}
				if t.PF32 != nil {
					pf32 := *t.PF32
					res = append(res, strconv.FormatFloat(float64(pf32), 'f', -1, 32))
				} else {
					res = append(res, "")
				}
				if t.PF64 != nil {
					pf64 := *t.PF64
					res = append(res, strconv.FormatFloat(pf64, 'f', -1, 64))
				} else {
					res = append(res, "")
				}
				if pt2 := t.PT2; pt2 != nil {
					res = append(res, pt2.T.String())
					if pt2.PT != nil {
						res = append(res, pt2.PT.String())
					} else {
						res = append(res, "")
					}
					if pt2.S != nil {
						res = append(res, *pt2.S)
					} else {
						res = append(res, "")
					}
				} else {
					res = append(res, "", "", "")
				}
				res = append(res, t.T2.T.String())
				if t.T2.PT != nil {
					res = append(res, t.T2.PT.String())
				} else {
					res = append(res, "")
				}
				if t.T2.S != nil {
					res = append(res, *t.T2.S)
				} else {
					res = append(res, "")
				}
				ai := make([]string, 0, len(t.AI))
				for _, i := range t.AI {
					str := strconv.FormatInt(int64(i), 10)
					ai = append(ai, str)
				}
				res = append(res, fmt.Sprintf("-[%s]-", strings.Join(ai, ",")))
				api := make([]string, 0, len(t.AI))
				for _, i := range t.API {
					str := ""
					if i != nil {
						str = strconv.FormatInt(int64(*i), 10)
					}
					ai = append(ai, str)
				}
				res = append(res, fmt.Sprintf("-[%s]-", strings.Join(api, ",")))
				resch <- func() ([]string, error) {
					return res, nil
				}
			}
		}
	}()
	return ch, resch, stop
}

// finih implementing Wed 16 Mar 2022 23:26:56 WIB

func BenchmarkCSVHandwritten(b *testing.B) {
	var ser Serializer = &handWritten{}
	for _, binput := range btable {
		b.Run(fmt.Sprintf("input_size_%d", len(binput.input)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				i, r, stop := ser.Start(context.Background(), true)
				go func() {
					for _, input := range binput.input {
						i <- input
					}
					err := stop()
					if err != nil {
						b.Error(err)
					}
				}()
				for fn := range r {
					_, err := fn()
					if err != nil {
						b.Error(err)
					}
				}
			}
		})
	}
}
