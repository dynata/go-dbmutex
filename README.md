# go-dbmutex

Package dbmutex implements a DB-based mutex that can be used to synchronize work
among multiple processes.

### Features
- works with mysql and postgres
- should work with database failovers (haven't verified this)
- mutex lock and unlock operations can timeout if desired
- callers can be notified when mutexes are unlocked
- minimal dependencies

### Install
`go get github.com/dynata/go-dbmutex`

### Documentation
After cloning run `godoc` and open a browser to http://localhost:6060
