package client

import "io"

type reader struct {
	r   io.Reader
	buf []byte
	off int
	n   int
}

func (r *reader) Read() ([]byte, error) {
	if r.off >= 0 {
		off := r.off
		r.off = -1
		return r.buf[off:r.n], nil
	}
	var err error
	r.n, err = r.r.Read(r.buf)
	return r.buf[:r.n], err
}
