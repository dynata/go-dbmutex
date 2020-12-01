package dbmerr

import (
	"fmt"
	"strings"
)

type msgErr string

func (e msgErr) Error() string {
	return string(e)
}

func newMsgErr(mainMsg, tableName, mutexName string, err error) msgErr {
	return msgErr(formatMsg(mainMsg, tableName, mutexName, err))
}

func formatMsg(mainMsg, tableName, mutexName string, err error) string {
	var xtra []string
	if tableName != "" {
		xtra = append(xtra, "tableName: "+tableName)
	}
	if mutexName != "" {
		xtra = append(xtra, "mutexName: "+mutexName)
	}
	if err != nil {
		errMsg := err.Error()
		if errMsg != "" {
			xtra = append(xtra, "error: "+errMsg)
		}
	}
	return mainMsg + plusExtraMsg(strings.Join(xtra, ", "))
}

const (
	mutexUninitializedMsg           = "Mutex uninitialized; use New"
	driverDetectionMsg              = "failed to detect driver"
	driverAllowedConcurrentLocksMsg = "driver allowed concurrent locks; check driver implementation"
	missingMutexTableMsg            = "mutex table does not exist"
	createMutexTableMsg             = "failed to create mutex table"
	lockFailFastMsg                 = "fast failure while locking"
	unlockFailFastMsg               = "fast failure while unlock"
	cannotObtainMutexEntryMsg       = "cannot obtain mutex entry"
	lockMsg                         = "failure while locking"
	unlockMsg                       = "failure while unlocking"
	refreshLockMsg                  = "failure refreshing locked mutex"
	notLockedMsg                    = "mutex is not locked"
)

// MutexUninitializedError is returned when caller attempts to use a Mutex that was not created via New.
type MutexUninitializedError struct{}

func (e MutexUninitializedError) Error() string {
	return mutexUninitializedMsg
}

// Allocate an MutexUninitializedError
func NewMutexUninitializedError() MutexUninitializedError {
	return MutexUninitializedError{}
}

// DriverDetectionError is returned when we are unable to detected the driver type.
type DriverDetectionError struct {
	msgErr
}

// Allocate a DriverDetectionError.
func NewDriverDetectionError(err error) DriverDetectionError {
	return DriverDetectionError{
		newMsgErr(driverDetectionMsg, "", "", err),
	}
}

// DriverAllowedConcurrentLocksError is returned when the Driver allows concurrent locks. This indicates
// a bug in the Driver implementation.
type DriverAllowedConcurrentLocksError struct {
	msgErr
}

// Allocate a DriverAllowedConcurrentLocksError.
func NewDriverAllowedConcurrentLocksError(tableName, mutexName string) DriverAllowedConcurrentLocksError {
	return DriverAllowedConcurrentLocksError{
		newMsgErr(driverAllowedConcurrentLocksMsg, tableName, mutexName, nil),
	}
}

// MissingMutexTableError is returned when we are unable to access the mutex table.
type MissingMutexTableError struct {
	msgErr
}

// Allocate a MissingMutexTableError.
func NewMissingMutexTableError(tableName string, err error) MissingMutexTableError {
	return MissingMutexTableError{
		newMsgErr(missingMutexTableMsg, tableName, "", err),
	}
}

// CreateMutexTableError is returned when we are unable to create the mutex table.
type CreateMutexTableError struct {
	msgErr
}

// Allocate a CreateMutexTableError.
func NewCreateMutexTableError(tableName string, err error) CreateMutexTableError {
	return CreateMutexTableError{
		newMsgErr(createMutexTableMsg, tableName, "", err),
	}
}

// LockFailFastError is returned when an error occurs during Lock and the caller requested that the
// Lock operation fail fast.
type LockFailFastError struct {
	msgErr
}

// Allocate a LockFailFastError.
func NewLockFailFastError(tableName, mutexName string, err error) LockFailFastError {
	return LockFailFastError{
		newMsgErr(lockFailFastMsg, tableName, mutexName, err),
	}
}

// UnlockFailFastError is returned when an error occurred during Unlock operation and the caller requested
// that Unlock should fail fast.
type UnlockFailFastError struct {
	msgErr
}

// Allocate a UnlockFailFastError.
func NewUnlockFailFastError(tableName, mutexName string, err error) UnlockFailFastError {
	return UnlockFailFastError{
		newMsgErr(unlockFailFastMsg, tableName, mutexName, err),
	}
}

// CannotObtainMutexEntryError is returned when we are unable to locate or insert a row for a named mutex.
type CannotObtainMutexEntryError struct {
	msgErr
}

// Allocate a CannotObtainMutexEntryError.
func NewCannotObtainMutexEntryError(tableName, mutexName string, err error) CannotObtainMutexEntryError {
	return CannotObtainMutexEntryError{
		newMsgErr(cannotObtainMutexEntryMsg, tableName, mutexName, err),
	}
}

// LockError is returned when we are unable to lock a mutex. This is normally a transient error.
type LockError struct {
	msgErr
}

// Allocate a LockError.
func NewLockError(tableName, mutexName string, err error) LockError {
	return LockError{
		msgErr(formatMsg(lockMsg, tableName, mutexName, err)),
	}
}

// UnlockError is returned when we are unable to unlock a mutex. This is normally a transient error.
type UnlockError struct {
	msgErr
}

// Allocate an UnlockError.
func NewUnlockError(tableName, mutexName string, err error) UnlockError {
	return UnlockError{
		msgErr(formatMsg(unlockMsg, tableName, mutexName, err)),
	}
}

// RefreshLockError is returned when we are unable to refresh a locked mutex. WillRetry can be used to check
// if the refresh will be retried.
type RefreshLockError struct {
	msgErr
	willRetry bool
}

// Allocate a RefreshLockError
func NewRefreshLockError(tableName, mutexName string, willRetry bool, err error) RefreshLockError {
	return RefreshLockError{
		msgErr: msgErr(formatMsg(refreshLockMsg, tableName, mutexName, err) +
			fmt.Sprintf(", willRetry: %t", willRetry)),
		willRetry: willRetry,
	}
}

// WillRetry returns true if the refresh operation will be retried.
func (e RefreshLockError) WillRetry() bool {
	return e.willRetry
}

// NotLockedError is returned when Unlock is called on a lock that is not held.
type NotLockedError struct {
	msgErr
}

// Allocate a NotLockedError
func NewNotLockedError(tableName, mutexName string) NotLockedError {
	return NotLockedError{
		newMsgErr(notLockedMsg, tableName, mutexName, nil),
	}
}

func plusExtraMsg(msg string) string {
	if msg != "" {
		return "; " + msg
	}
	return ""
}
