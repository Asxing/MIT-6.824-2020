package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import "../mr"
import "time"

func main() {
	//if len(os.Args) < 2 {
	//	fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
	//	os.Exit(1)
	//}
	files := []string{
		"/Users/zeyangg/SynologyDrive/ww/6.824/src/main/pg-being_ernest.txt",
		"/Users/zeyangg/SynologyDrive/ww/6.824/src/main/pg-dorian_gray.txt",
		"/Users/zeyangg/SynologyDrive/ww/6.824/src/main/pg-frankenstein.txt",
		"/Users/zeyangg/SynologyDrive/ww/6.824/src/main/pg-grimm.txt",
		"/Users/zeyangg/SynologyDrive/ww/6.824/src/main/pg-huckleberry_finn.txt",
		"/Users/zeyangg/SynologyDrive/ww/6.824/src/main/pg-metamorphosis.txt",
		"/Users/zeyangg/SynologyDrive/ww/6.824/src/main/pg-sherlock_holmes.txt",
		"/Users/zeyangg/SynologyDrive/ww/6.824/src/main/pg-tom_sawyer.txt"}
	//m := mr.MakeMaster(os.Args[1:], 10)
	m := mr.MakeMaster(files, 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
