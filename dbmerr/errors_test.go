package dbmerr

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestNewErrors(t *testing.T) {
	const testTable = "testTable"
	const testMutex = "testMutex"
	testErr := errors.New("some other error")
	type testDef struct {
		allocator  func(tableName, mutexName string, err error) error
		msgNilErr  string
		msgPlusErr string
		expect     reflect.Value
	}
	tests := []testDef{
		{
			allocator: func(tableName, mutexName string, err error) error {
				return NewMutexUninitializedError()
			},
			msgNilErr:  formatMsg(mutexUninitializedMsg, "", "", nil),
			msgPlusErr: formatMsg(mutexUninitializedMsg, "", "", nil),
			expect:     reflect.ValueOf(MutexUninitializedError{}),
		},
		{
			allocator: func(tableName, mutexName string, err error) error {
				return NewDriverDetectionError(err)
			},
			msgNilErr:  formatMsg(driverDetectionMsg, "", "", nil),
			msgPlusErr: formatMsg(driverDetectionMsg, "", "", testErr),
			expect:     reflect.ValueOf(DriverDetectionError{}),
		},
		{
			allocator: func(tableName, mutexName string, err error) error {
				return NewDriverAllowedConcurrentLocksError(tableName, mutexName)
			},
			msgNilErr:  formatMsg(driverAllowedConcurrentLocksMsg, testTable, testMutex, nil),
			msgPlusErr: formatMsg(driverAllowedConcurrentLocksMsg, testTable, testMutex, nil),
			expect:     reflect.ValueOf(DriverAllowedConcurrentLocksError{}),
		},
		{
			allocator: func(tableName, mutexName string, err error) error {
				return NewMissingMutexTableError(tableName, err)
			},
			msgNilErr:  formatMsg(missingMutexTableMsg, testTable, "", nil),
			msgPlusErr: formatMsg(missingMutexTableMsg, testTable, "", testErr),
			expect:     reflect.ValueOf(MissingMutexTableError{}),
		},
		{
			allocator: func(tableName, mutexName string, err error) error {
				return NewCreateMutexTableError(tableName, err)
			},
			msgNilErr:  formatMsg(createMutexTableMsg, testTable, "", nil),
			msgPlusErr: formatMsg(createMutexTableMsg, testTable, "", testErr),
			expect:     reflect.ValueOf(CreateMutexTableError{}),
		},
		{
			allocator: func(tableName, mutexName string, err error) error {
				return NewLockFailFastError(tableName, mutexName, err)
			},
			msgNilErr:  formatMsg(lockFailFastMsg, testTable, testMutex, nil),
			msgPlusErr: formatMsg(lockFailFastMsg, testTable, testMutex, testErr),
			expect:     reflect.ValueOf(LockFailFastError{}),
		},
		{
			allocator: func(tableName, mutexName string, err error) error {
				return NewUnlockFailFastError(tableName, mutexName, err)
			},
			msgNilErr:  formatMsg(unlockFailFastMsg, testTable, testMutex, nil),
			msgPlusErr: formatMsg(unlockFailFastMsg, testTable, testMutex, testErr),
			expect:     reflect.ValueOf(UnlockFailFastError{}),
		},
		{
			allocator: func(tableName, mutexName string, err error) error {
				return NewCannotObtainMutexEntryError(tableName, mutexName, err)
			},
			msgNilErr:  formatMsg(cannotObtainMutexEntryMsg, testTable, testMutex, nil),
			msgPlusErr: formatMsg(cannotObtainMutexEntryMsg, testTable, testMutex, testErr),
			expect:     reflect.ValueOf(CannotObtainMutexEntryError{}),
		},
		{
			allocator: func(tableName, mutexName string, err error) error {
				return NewLockError(tableName, mutexName, err)
			},
			msgNilErr:  formatMsg(lockMsg, testTable, testMutex, nil),
			msgPlusErr: formatMsg(lockMsg, testTable, testMutex, testErr),
			expect:     reflect.ValueOf(LockError{}),
		},
		{
			allocator: func(tableName, mutexName string, err error) error {
				return NewUnlockError(tableName, mutexName, err)
			},
			msgNilErr:  formatMsg(unlockMsg, testTable, testMutex, nil),
			msgPlusErr: formatMsg(unlockMsg, testTable, testMutex, testErr),
			expect:     reflect.ValueOf(UnlockError{}),
		},
		{
			allocator: func(tableName, mutexName string, err error) error {
				return NewRefreshLockError(tableName, mutexName, true, err)
			},
			msgNilErr:  formatMsg(refreshLockMsg, testTable, testMutex, nil) + ", willRetry: true",
			msgPlusErr: formatMsg(refreshLockMsg, testTable, testMutex, testErr) + ", willRetry: true",
			expect:     reflect.ValueOf(RefreshLockError{}),
		},
		{
			allocator: func(tableName, mutexName string, err error) error {
				return NewNotLockedError(tableName, mutexName)
			},
			msgNilErr:  formatMsg(notLockedMsg, testTable, testMutex, nil),
			msgPlusErr: formatMsg(notLockedMsg, testTable, testMutex, nil),
			expect:     reflect.ValueOf(NotLockedError{}),
		},
	}

	for _, tt := range tests {
		tt := tt
		shortTypeName := strings.Join(strings.SplitN(tt.expect.Type().String(), ".", 2)[1:], "")
		t.Run(shortTypeName+"_nil_err", func(t *testing.T) {
			createdErr := tt.allocator(testTable, testMutex, nil)
			// test to see if errors.As call treats returned error as the correct type.
			ptr := reflect.New(tt.expect.Type())
			if !errors.As(createdErr, ptr.Elem().Addr().Interface()) {
				t.Errorf("expected error to be of type %s but was not", tt.expect.Type().String())
				return
			}
			// now cast to error and see if message looks good
			if errVal, ok := ptr.Elem().Interface().(error); ok {
				errMsg := errVal.Error()
				if tt.msgNilErr != errMsg {
					t.Errorf("expected '%s' but got '%s'", tt.msgNilErr, errMsg)
				}
			} else {
				t.Errorf("expected type %s is not an error", tt.expect.Type().String())
			}
		})
		t.Run(shortTypeName+"_plus_err", func(t *testing.T) {
			createdErr := tt.allocator(testTable, testMutex, testErr)
			// test to see if errors.As call treats returned error as the correct type.
			ptr := reflect.New(tt.expect.Type())
			if !errors.As(createdErr, ptr.Elem().Addr().Interface()) {
				t.Errorf("expected error to be of type %s but was not", tt.expect.Type().String())
				return
			}
			// now cast to error and see if message looks good
			if errVal, ok := ptr.Elem().Interface().(error); ok {
				errMsg := errVal.Error()
				// t.Log(errMsg)
				if tt.msgPlusErr != errMsg {
					t.Errorf("expected '%s' but got '%s'", tt.msgPlusErr, errMsg)
				}
			} else {
				t.Errorf("expected type %s is not an error", tt.expect.Type().String())
			}
		})
	}
}

func Test_formatMsg(t *testing.T) {
	type args struct {
		mainMsg   string
		tableName string
		mutexName string
		err       error
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "everything blank",
			args: args{},
			want: "",
		},
		{
			name: "only main msg",
			args: args{
				mainMsg: "mm",
			},
			want: "mm",
		},
		{
			name: "main msg and table",
			args: args{
				mainMsg:   "mm",
				tableName: "tn",
			},
			want: "mm; tableName: tn",
		},
		{
			name: "main msg, table, lock",
			args: args{
				mainMsg:   "mm",
				tableName: "tn",
				mutexName: "mn",
			},
			want: "mm; tableName: tn, mutexName: mn",
		},
		{
			name: "main msg, table, lock, err",
			args: args{
				mainMsg:   "mm",
				tableName: "tn",
				mutexName: "mn",
				err:       errors.New("e"),
			},
			want: "mm; tableName: tn, mutexName: mn, error: e",
		},
		{
			name: "main msg and err",
			args: args{
				mainMsg: "mm",
				err:     errors.New("e"),
			},
			want: "mm; error: e",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatMsg(tt.args.mainMsg, tt.args.tableName, tt.args.mutexName, tt.args.err); got != tt.want {
				t.Errorf("formatMsg() = %v, want %v", got, tt.want)
			}
		})
	}
}
