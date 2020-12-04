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

[![PkgGoDev](https://pkg.go.dev/badge/github.com/dynata/go-dbmutex)](https://pkg.go.dev/github.com/dynata/go-dbmutex)

Full `godoc` style documentation for the package can be viewed online without installing this package by
using the [GoDoc site](https://pkg.go.dev/github.com/dynata/go-dbmutex).

### Disclaimer

YOU ARE USING THIS APPLICATION AT YOUR OWN RISK. DYNATA MAKES NO WARRANTIES OR REPRESENTATIONS ABOUT THIS APPLICATION.
THIS APPLICATION IS PROVIDED TO YOU “AS IS”. DYNATA HEREBY DISCLAIMS ALL WARRANTIES, EXPRESS OR IMPLIED WITH RESPECT
TO THE APPLICATION, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS OF PURPOSE,
NON-INFRINGEMENT AND ANY IMPLIED WARRANTIES ARISING OUT OF A COURSE OF PERFORMANCE, DEALING, OR TRADE USAGE.
TO THE EXTENT DYNATA MAY NOT, AS A MATTER OF APPLICABLE LAW, DISCLAIM ANY WARRANTY, THE SCOPE AND DURATION OF SUCH
WARRANTY SHALL BE LIMITED TO THE MINIMUM PERMITTED UNDER SUCH APPLICABLE LAW.  YOU WILL INDEMNIFY, DEFEND AND HOLD
HARMLESS DYNATA AND ITS AFFILIATES, EMPLOYEES, OFFICERS AND CONTRACTORS FROM ANY THIRD PARTY CLAIM ARISING FROM
YOUR USE OF THIS APPLICATION.
