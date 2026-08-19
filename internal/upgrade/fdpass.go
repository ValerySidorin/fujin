//go:build unix

package upgrade

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"syscall"
)

// SendFDs sends file descriptors over a Unix domain socket connection using SCM_RIGHTS.
// The metadata is sent as the message payload alongside the FDs.
func SendFDs(conn *net.UnixConn, files []*os.File, metadata []byte) error {
	if len(files) == 0 {
		return fmt.Errorf("no file descriptors to send")
	}

	fds := make([]int, len(files))
	for i, f := range files {
		fds[i] = int(f.Fd())
	}

	rights := syscall.UnixRights(fds...)

	// Prefix metadata with length for framing
	buf := make([]byte, 4+len(metadata))
	binary.BigEndian.PutUint32(buf, uint32(len(metadata)))
	copy(buf[4:], metadata)

	_, _, err := conn.WriteMsgUnix(buf, rights, nil)
	if err != nil {
		return fmt.Errorf("send fds: %w", err)
	}

	return nil
}

// RecvFDs receives file descriptors over a Unix domain socket connection using SCM_RIGHTS.
// Returns the received files and metadata payload.
func RecvFDs(conn *net.UnixConn, maxFDs int) ([]*os.File, []byte, error) {
	buf := make([]byte, 64*1024)
	oob := make([]byte, syscall.CmsgLen(maxFDs*4))

	n, oobn, _, _, err := conn.ReadMsgUnix(buf, oob)
	if err != nil {
		return nil, nil, fmt.Errorf("recv fds: %w", err)
	}

	// Parse metadata (length-prefixed)
	if n < 4 {
		return nil, nil, fmt.Errorf("metadata too short: %d bytes", n)
	}
	metaLen := binary.BigEndian.Uint32(buf[:4])
	if int(metaLen) > n-4 {
		return nil, nil, fmt.Errorf("metadata length mismatch: declared %d, available %d", metaLen, n-4)
	}
	metadata := make([]byte, metaLen)
	copy(metadata, buf[4:4+metaLen])

	// Parse file descriptors from OOB data
	scms, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return nil, nil, fmt.Errorf("parse control message: %w", err)
	}

	var files []*os.File
	for _, scm := range scms {
		fds, err := syscall.ParseUnixRights(&scm)
		if err != nil {
			return nil, nil, fmt.Errorf("parse unix rights: %w", err)
		}
		for i, fd := range fds {
			f := os.NewFile(uintptr(fd), fmt.Sprintf("inherited-fd-%d", i))
			if f == nil {
				return nil, nil, fmt.Errorf("invalid fd %d", fd)
			}
			files = append(files, f)
		}
	}

	return files, metadata, nil
}
