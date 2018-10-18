package main

import (
    "os" 
    "flag"
    "log"
    "fmt"
    "github.com/streadway/amqp"
    "strconv"
    "bufio"
    "io"
    "time"
    "encoding/json"
    "runtime"
    "encoding/csv"
    "runtime/pprof"
)

const (
	STEREOTYPE_COMMAND = iota 
	STEREOTYPE_RESULT = iota  
)

const EXCHANGE_NAME = "exchange"
const MASTER_QUEUE_NAME = "master"
const MASTER_BINDING_KEY = "master"
const MESSAGE_CHECK_DELAY = 1000000000
const SLAVE_MESSAGE_HANDLE_LIMIT = 1

type Message struct
{
  Stereotype int
  Payload Matrix2D
  SenderID string
}

type Matrix2D struct
{
  Rows uint64
  Columns uint64
  Data [][]float64
}

func main() {
    
    var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
    var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

    flag.Parse()
    if *cpuprofile != "" {
        f, err := os.Create(*cpuprofile)
        if err != nil {
            log.Fatal(err)
        }
        cpuerr := pprof.StartCPUProfile(f)
        if cpuerr != nil {
            fmt.Println("Start CPU Profile error: ", cpuerr)
        }
        
        defer pprof.StopCPUProfile()
    }
        
    role := os.Getenv("APP_ROLE");
    nodeNumber := os.Getenv("NODE_NUMBER");
    
    var messageCount uint64 = 5;
    
    if role == "MASTER" {
      matrix := readMatrixFrom("/data/data.csv")
    
      q, ch, conn := setupAMQP(MASTER_QUEUE_NAME, MASTER_BINDING_KEY)
      
      var counter uint64 = 0
      for { 
        s := strconv.FormatUint(counter, 8)
        message := &Message{ Stereotype: STEREOTYPE_COMMAND, Payload: *matrix }
        sendMessage(message,q,ch, "slave"+s)
        counter++
        
        if counter == uint64(messageCount) {
          break
        }
      }
    
      answerReceived := uint64(0)
      for { 
        log.Printf("Master checking for messages")
        msgs := receiveMessage(q,ch)
        
        go func() {
          for d := range msgs {
            answerReceived++
            var message Message
            json.Unmarshal(d.Body,&message)
            log.Printf("Master received a message: %s", d.Body)
            log.Printf("Master received a message: %s", message.Stereotype)
          }
        }()
                
        if answerReceived == messageCount {
          break
        } else {
          time.Sleep(MESSAGE_CHECK_DELAY)
        }
      }
            
      defer conn.Close()
    }
    
    if role == "SLAVE" {
      q, ch, conn := setupAMQP("","slave"+nodeNumber)
      
      hostname, _ := os.Hostname()
      handledMessageCount := uint64(0)
      for { 
        msgs := receiveMessage(q,ch)
        
        go func() {
          for d := range msgs {
            handledMessageCount++
            var message Message
            json.Unmarshal(d.Body,&message)
                  
            var transformed *Matrix2D = processPayload(&(message.Payload))
            message2 := &Message{ Stereotype: STEREOTYPE_RESULT, SenderID: hostname+":"+nodeNumber, Payload: *transformed }
            sendMessage(message2,q,ch,MASTER_BINDING_KEY)
          }
        }()
                
        if handledMessageCount == SLAVE_MESSAGE_HANDLE_LIMIT {
          break
        } else {
          time.Sleep(MESSAGE_CHECK_DELAY)
        }
      }
      
      defer conn.Close()
    } 
    
    if *memprofile != "" {
        f, err := os.Create(*memprofile)
        if err != nil {
            log.Fatal("could not create memory profile: ", err)
        }
        runtime.GC() // get up-to-date statistics
        if err := pprof.WriteHeapProfile(f); err != nil {
            log.Fatal("could not write memory profile: ", err)
        }
        f.Close()
    }
    
}

func make2DFloat64Array(rows uint64, columns uint64) ([][]float64) {
  var data [][]float64 = make([][]float64, rows)
  
  for i:=uint64(0);i<rows;i++ {
    data[i] = make([]float64, columns)
  }
  
  return data
}

func readMatrixFrom(filename string) (*Matrix2D) {
  f, _ := os.Open(filename)
  r := csv.NewReader(bufio.NewReader(f))
  row := uint64(0)
  rows := uint64(0)
  columns := uint64(0)
  var data [][]float64
  
  for {
      record, err := r.Read()
      // Stop at EOF.
      if err == io.EOF {
          break
      }
      
      if row==0 {
        rows,_ = strconv.ParseUint(record[0], 10, 64)
        columns,_ = strconv.ParseUint(record[1], 10, 64)
        data = make2DFloat64Array(rows,columns)
        row++
        continue
      }
      
      for i:=uint64(0);i<columns;i++ {
        data[row-1][i],_ = strconv.ParseFloat(record[i], 64)
      }
      
      row++
  }
  
  return &Matrix2D{Data: data, Rows: rows, Columns: columns}
}

func setupAMQP(queueName string, bindingKey string) (amqp.Queue,*amqp.Channel,*amqp.Connection) {
    conn, err := amqp.Dial("amqp://admin:secret@messagebus:5672/")
    failOnError(err, "Failed to connect to RabbitMQ")
    
    ch, err := conn.Channel()
    failOnError(err, "Failed to open a channel")
  
    q, err := ch.QueueDeclare(
      queueName, // name
      false,   // durable
      true,   // delete when unused
      false,   // exclusive
      false,   // no-wait
      nil,     // arguments
    )
    failOnError(err, "Failed to declare a master queue")
    
    err = ch.ExchangeDeclare(
          EXCHANGE_NAME,     // name
          "direct", // type
          true,          // durable
          true,         // auto-deleted
          false,         // internal
          false,         // noWait
          nil,           // arguments
    )
    failOnError(err, "Failed to declare an exchange")
    
    err = ch.QueueBind(
    		queueName,    // name of the queue
    		bindingKey,      // bindingKey
    		EXCHANGE_NAME, // sourceExchange
    		false,    // noWait
    		nil,      // arguments
    );
    failOnError(err, "Failed to bind a master queue")
    
    return q,ch,conn
}

func copyMatrix2D(input *Matrix2D) (*Matrix2D) {
  output := Matrix2D{Data: input.Data, Rows: input.Rows, Columns: input.Columns}
  copy(output.Data, input.Data)
  return &output
}

func processPayload(payload *Matrix2D) (*Matrix2D) {
  var output *Matrix2D = copyMatrix2D(payload)
  return output
}

func receiveMessage(q amqp.Queue, ch *amqp.Channel) (<-chan amqp.Delivery) {
    msgs, err := ch.Consume(
      q.Name, // queue
      "",     // consumer
      true,   // auto-ack
      false,  // exclusive
      false,  // no-local
      false,  // no-wait
      nil,    // args
    )
    
    failOnError(err, "Failed to register a consumer")
    
    return msgs;
}

func sendMessage(message *Message, q amqp.Queue, ch *amqp.Channel, routingKey string) {
    body, _ := json.Marshal(message)
    log.Printf("publishing %dB body (%s), routing key %s", len(body), body,routingKey)
    
    err := ch.Publish(
      EXCHANGE_NAME,     // exchange
      routingKey, // routing key
      false,  // mandatory
      false,  // immediate
      amqp.Publishing {
        ContentType: "application/json",
        Body:        []byte(body),
      })
      
    failOnError(err, "Failed to publish a message")
}

func failOnError(err error, msg string) {
  if err != nil {
    log.Fatalf("%s: %s", msg, err)
    panic(fmt.Sprintf("%s: %s", msg, err))
  }
}