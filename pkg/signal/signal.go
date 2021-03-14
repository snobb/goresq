package signal

import (
	"os"
	"os/signal"
	"syscall"
)

type QuitChannel <-chan struct{}

func Quit() QuitChannel {
	sigs := make(chan os.Signal)
	done := make(chan struct{})

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)

	go func() {
		<-sigs
		done <- struct{}{}
	}()

	return done
}
