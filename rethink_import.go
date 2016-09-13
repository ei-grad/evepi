package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	r "gopkg.in/dancannon/gorethink.v2"
	"log"
	"os"
	"runtime"
)

type chunk [1000]map[string]interface{}

func Writer(ch chan chunk, done chan bool) {

	session, err := r.Connect(r.ConnectOpts{
	//Address: "localhost",
	})
	if err != nil {
		log.Fatalln(err)
	}

	t := r.DB("eve").Table("MarketOrders")

	for i := range ch {
		r_err := t.Insert(i).Exec(session)
		if r_err != nil {
			log.Fatalln(r_err)
		}
		fmt.Print("C")
	}

	done <- true
}

func main() {

	file, err := os.Open("orders.json")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	ch := make(chan chunk)
	done := make(chan bool)

	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		go Writer(ch, done)
	}

	buf := chunk{}
	i := 0

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {

		err := json.Unmarshal([]byte(scanner.Text()), &buf[i])
		if err != nil {
			log.Fatalln(err)
		}

		i++

		if i%100 == 99 {
			fmt.Print(".")
		}

		if i == cap(buf) {
			fmt.Println("F")
			ch <- buf
			i = 0
		}

	}

	close(ch)

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		<-done
	}

	close(done)

}
