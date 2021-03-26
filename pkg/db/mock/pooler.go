// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mock

import (
	"github.com/snobb/goresq/pkg/db"
	"sync"
)

// Ensure, that PoolerMock does implement db.Pooler.
// If this is not the case, regenerate this file with moq.
var _ db.Pooler = &PoolerMock{}

// PoolerMock is a mock implementation of db.Pooler.
//
// 	func TestSomethingThatUsesPooler(t *testing.T) {
//
// 		// make and configure a mocked db.Pooler
// 		mockedPooler := &PoolerMock{
// 			CloseFunc: func() error {
// 				panic("mock out the Close method")
// 			},
// 			ConnFunc: func() (db.Conn, error) {
// 				panic("mock out the Conn method")
// 			},
// 		}
//
// 		// use mockedPooler in code that requires db.Pooler
// 		// and then make assertions.
//
// 	}
type PoolerMock struct {
	// CloseFunc mocks the Close method.
	CloseFunc func() error

	// ConnFunc mocks the Conn method.
	ConnFunc func() (db.Conn, error)

	// calls tracks calls to the methods.
	calls struct {
		// Close holds details about calls to the Close method.
		Close []struct {
		}
		// Conn holds details about calls to the Conn method.
		Conn []struct {
		}
	}
	lockClose sync.RWMutex
	lockConn  sync.RWMutex
}

// Close calls CloseFunc.
func (mock *PoolerMock) Close() error {
	if mock.CloseFunc == nil {
		panic("PoolerMock.CloseFunc: method is nil but Pooler.Close was just called")
	}
	callInfo := struct {
	}{}
	mock.lockClose.Lock()
	mock.calls.Close = append(mock.calls.Close, callInfo)
	mock.lockClose.Unlock()
	return mock.CloseFunc()
}

// CloseCalls gets all the calls that were made to Close.
// Check the length with:
//     len(mockedPooler.CloseCalls())
func (mock *PoolerMock) CloseCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockClose.RLock()
	calls = mock.calls.Close
	mock.lockClose.RUnlock()
	return calls
}

// Conn calls ConnFunc.
func (mock *PoolerMock) Conn() (db.Conn, error) {
	if mock.ConnFunc == nil {
		panic("PoolerMock.ConnFunc: method is nil but Pooler.Conn was just called")
	}
	callInfo := struct {
	}{}
	mock.lockConn.Lock()
	mock.calls.Conn = append(mock.calls.Conn, callInfo)
	mock.lockConn.Unlock()
	return mock.ConnFunc()
}

// ConnCalls gets all the calls that were made to Conn.
// Check the length with:
//     len(mockedPooler.ConnCalls())
func (mock *PoolerMock) ConnCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockConn.RLock()
	calls = mock.calls.Conn
	mock.lockConn.RUnlock()
	return calls
}