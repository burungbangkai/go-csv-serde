package csvserde

type options struct {
	delimiter            rune
	quote                rune
	tagName              string
	bufferSize           int
	sliceOrArrayEncloser [2]rune
}

type SerdeOption func(*options)

func WithDelimiter(delimiter rune) SerdeOption {
	return func(opts *options) {
		opts.delimiter = delimiter
	}
}

func WithQuote(quote rune) SerdeOption {
	return func(opts *options) {
		opts.quote = quote
	}
}

func WithTagName(tag string) SerdeOption {
	return func(opts *options) {
		opts.tagName = tag
	}
}

func WithBufferSize(size int) SerdeOption {
	return func(opts *options) {
		if size > 0 {
			opts.bufferSize = size
		}
	}
}

func WithSliceOrArrayEncloser(encloser [2]rune) SerdeOption {
	return func(opts *options) {
		opts.sliceOrArrayEncloser = encloser
	}
}
