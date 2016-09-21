package main

import (
    "bufio"
    "encoding/json"
    "fmt"
    r "gopkg.in/dancannon/gorethink.v2"
    "log"
    "os"
)

const NUM_WRITERS = 64
const CHUNKSIZE = 256

type chunk []map[string]interface{}

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

    for i := 0; i < NUM_WRITERS; i++ {
        go Writer(ch, done)
    }

    buf := chunk{}
    i := 0

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {

        var doc map[string]interface{}

        //err := json.Unmarshal([]byte(scanner.Text()), &buf[i])
        err := json.Unmarshal([]byte(scanner.Text()), &doc)
        if err != nil {
            log.Fatalln(err)
        }
        buf = append(buf, doc)

        i++

        if i == CHUNKSIZE {
            fmt.Print("F")
            ch <- buf
            buf = chunk{}
            i = 0
        }

    }

    ch <- buf

    close(ch)

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }

    for i := 0; i < NUM_WRITERS; i++ {
        <-done
    }

    close(done)

}
