//go:build !darwin && !linux

package cymule

import "fmt"

func engineWaitAuthorityAvailable() bool { return false }

func engineProcessExitedWithoutReaping(_ int) (bool, error) {
	return false, fmt.Errorf("Engine process wait authority is unsupported on this platform")
}
