package client

import (
	"net"
	"time"
	"os"
	"bufio"
	"errors"
	"sync/atomic"
	"io"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/util"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
)

const CONNECTION_ATTEMPTS_MAX = 5	
const CONNECTION_ATTEMPS_DELAY_MS = 1000


type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile string
	OutputFile string
	BatchSize  string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
	shutdown atomic.Bool
}

func (client *Client) Shutdown() {
	client.shutdown.Store(true)
	client.conn.Close() 
}

func (client *Client) IsShuttingDown() bool {  
	return client.shutdown.Load()
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}


func (client *Client) Run() error {
	const mainAction = "test-echo-server"
	defer client.conn.Close()

	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	batchSize, err := util.ParseInt(client.config.BatchSize)

	if err != nil {
		errors.New("BATCH_SIZE inválido: " + client.config.BatchSize)
	}

	batch := make([]protocol.Bet, 0, batchSize)

	processBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		payload := protocol.Encode_bet_batch(client.config.AgencyId, batch)
		if err := protocol.Write_message(client.conn, protocol.MsgBetBatch, payload); err != nil {
			return err
		}
		msgType, _, err := protocol.Read_message(client.conn)
		if err != nil {
			return err
		}
		if msgType != protocol.MsgBatchAck {
			errors.New("unexpected response type " + util.ParseString(int(msgType)))
		}
		batch = batch[:0]
		return nil
	}


	scanner := bufio.NewScanner(inputFile)

	for scanner.Scan(){
		if client.shutdown.Load() {
			return nil 
		}

		line := scanner.Text()
		if line == "" {
			continue
		}

		fields := util.SplitFields(line, ',')

		if err != nil {
			errors.New("documento inválido en input: " + fields[2])
		}

		batch = append(batch, protocol.Bet{
			FirstName: fields[0], LastName: fields[1], Document: fields[2],
			Birthdate: fields[3], Number: fields[4],
		})

		if len(batch) >= batchSize {
			if err := processBatch(); err != nil {
				return err
			}
		}

	}
	
	if err := scanner.Err(); err != nil {
		return err
	}

	if err := processBatch(); err != nil { 
		return err
	}

	if err := protocol.Write_message(client.conn, protocol.MsgDone, nil); err != nil {
		return err
	}

	msgType, payload, err := protocol.Read_message(client.conn)
	if err != nil {
		return err
	}

	if msgType != protocol.MsgWinners {
		errors.New("unexpected response type " + util.ParseString(int(msgType)))
	}

	winnerDocuments := map[int]bool{}

	if len(payload) > 0 {
		for _, documentStr := range util.SplitFields(string(payload), ',') {
			document, err := util.ParseInt(documentStr) 
			if err != nil {
				continue
			}

			winnerDocuments[document] = true
		}
	}

	if len(winnerDocuments) == 0 {
		return nil
	}

	if _, err := inputFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	scanner = bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		fields := util.SplitFields(line, ',')
		documentNumber, err := util.ParseInt(fields[2])
		if err != nil {
			continue
		}
		if winnerDocuments[documentNumber] {
			outputFile.WriteString(line + "\n")
		}
	}
	return scanner.Err()

}
