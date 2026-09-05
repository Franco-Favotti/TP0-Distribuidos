package client

import (
	"net"
	"time"
	"os"
	"bufio"
	"fmt"
	"strings"
	"strconv"

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

	bets := map[int]string{}
	batch := []protocol.Bet{}
	batchSize, err := strconv.Atoi(client.config.BatchSize)

	if err != nil {
		return fmt.Errorf("BATCH_SIZE inválido: %q", client.config.BatchSize)
	}
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
			return fmt.Errorf("unexpected response type %d", msgType)
		}
		batch = batch[:0]
		return nil
	}


	scanner := bufio.NewScanner(inputFile)

	for scanner.Scan(){
		line := scanner.Text()
		if line == "" {
			continue
		}

		fields := strings.Split(line, ",")
		document, err := strconv.Atoi(fields[2])

		if err != nil {
			return fmt.Errorf("documento inválido en input: %q", fields[2])
		}

		bets[document] = line

		batch = append(batch, protocol.Bet{
			FirstName: fields[0], LastName: fields[1], Document: fields[2],
			Birthdate: fields[3], Number: fields[4],
		})

		if len(batch) >= batchSize {
			if err := processBatch(); err != nil {
				return err
			}
		}
		/*payload := protocol.Encode_bet_batch(client.config.AgencyId, []protocol.Bet{bet})

		if err := protocol.Write_message(client.conn, protocol.MsgBetBatch, payload); err != nil {
			return err
		}

		msgType, _, err := protocol.Read_message(client.conn)
		if err != nil {
			return err
		}

		if msgType != protocol.MsgBatchAck {
			return fmt.Errorf("unexpected response type %d", msgType)
		}*/
	
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
		return fmt.Errorf("unexpected response type %d", msgType)
	}

	if len(payload) > 0 {
		for _, documentStr := range strings.Split(string(payload), ",") {
			document, err := strconv.Atoi(documentStr) 
			if err != nil {
				continue
			}
			if line, ok := bets[document]; ok {
				outputFile.WriteString(line + "\n")
			}
		}
	}
	return nil
}
