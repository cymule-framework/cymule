//go:build linux

package cymule

import (
	"errors"
	"runtime"
	"syscall"
	"unsafe"
)

const (
	waitIDProcess = 1
	waitIDExited  = 4
	waitIDNoHang  = 1
	waitIDNoWait  = 0x01000000
)

func engineWaitAuthorityAvailable() bool { return true }

func engineProcessExitedWithoutReaping(processID int) (bool, error) {
	for {
		var information [128]byte
		_, _, callError := syscall.Syscall6(
			syscall.SYS_WAITID,
			waitIDProcess,
			uintptr(processID),
			uintptr(unsafe.Pointer(&information[0])),
			waitIDExited|waitIDNoHang|waitIDNoWait,
			0,
			0,
		)
		runtime.KeepAlive(&information)
		if callError == 0 {
			code := *(*int32)(unsafe.Pointer(&information[8]))
			return code == 1 || code == 2 || code == 3, nil
		}
		if errors.Is(callError, syscall.EINTR) {
			continue
		}
		return false, callError
	}
}
