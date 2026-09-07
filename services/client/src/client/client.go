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

//Setea en true el flag de shutdown y cierra la conexion para desbloquear Reads o Writes pendientes
func (client *Client) Shutdown() {
	client.shutdown.Store(true)
	client.conn.Close() 
}

//Indica si se solicito un apagado voluntario, para diferenciarlo de un error de comunicacion
func (client *Client) IsShuttingDown() bool {  
	return client.shutdown.Load()
}

//Conecta al servidor y arma la instancia del cliente
func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

//Reintenta la conexion TCP segun la variable CONNECTION_ATTEMPTS_MAX
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
		return errors.New("BATCH_SIZE inválido: " + client.config.BatchSize)
	}

	if err := client.sendBets(inputFile, batchSize); err != nil {
		return err
	}

	winnerDocuments, err := client.receiveWinners()
	if err != nil {
		return err
	}
	if len(winnerDocuments) == 0 {
		return nil
	}

	return writeWinningLines(inputFile, outputFile, winnerDocuments)
}

//Lee inputFile linea por linea, agrupa las apuestas en lotes de tamaño batchSize y los envia al servidor
func (client *Client) sendBets(inputFile *os.File, batchSize int) error {
	batch := make([]protocol.Bet, 0, batchSize)

	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		if client.shutdown.Load() {
			return nil
		}

		line := scanner.Text()
		if line == "" {
			continue
		}

		bet, err := parseBetLine(line)
		if err != nil {
			return err
		}
		batch = append(batch, bet)

		if len(batch) >= batchSize {
			newBatch, err := client.sendBatch(batch)
			if err != nil {
				return err
			}
			batch = newBatch
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	_, err := client.sendBatch(batch)
	return err
}

//convierte una linea del INPUT_FILE en un protocol.Bet
func parseBetLine(line string) (protocol.Bet, error) {
	fields := util.SplitFields(line, ',')
	if len(fields) != 5 {
		return protocol.Bet{}, errors.New("incorrect input line: " + line)
	}
	return protocol.Bet{
		FirstName: fields[0], LastName: fields[1], Document: fields[2],
		Birthdate: fields[3], Number: fields[4],
	}, nil
}

//Envia lote de apuestas y espera su BATCH_ACK. Devuelve el batch vaciado para seguir acumulando
func (client *Client) sendBatch(batch []protocol.Bet) ([]protocol.Bet, error) {
	if len(batch) == 0 {
		return batch, nil
	}
	payload := protocol.Encode_bet_batch(client.config.AgencyId, batch)
	if err := protocol.Write_message(client.conn, protocol.MsgBetBatch, payload); err != nil {
		return batch, err
	}
	msgType, _, err := protocol.Read_message(client.conn)
	if err != nil {
		return batch, err
	}
	if msgType != protocol.MsgBatchAck {
		return batch, errors.New("unexpected response type " + util.ParseString(int(msgType)))
	}
	return batch[:0], nil
}


//Avisa al servidor que esta agencia termino de enviar y espera la respuesta con los documentos ganadores
func (client *Client) receiveWinners() (map[int]bool, error) {
	if err := protocol.Write_message(client.conn, protocol.MsgDone, nil); err != nil {
		return nil, err
	}

	msgType, payload, err := protocol.Read_message(client.conn)
	if err != nil {
		return nil, err
	}
	if msgType != protocol.MsgWinners {
		return nil, errors.New("unexpected response type " + util.ParseString(int(msgType)))
	}

	winnerDocuments := map[int]bool{}
	if len(payload) == 0 {
		return winnerDocuments, nil
	}
	for _, documentStr := range util.SplitFields(string(payload), ',') {
		document, err := util.ParseInt(documentStr)
		if err != nil {
			continue
		}
		winnerDocuments[document] = true
	}
	return winnerDocuments, nil
}

//Relee inputFile desde el principio y copia a outputFile las lineas cuyo documento esta en winnerDocuments
func writeWinningLines(inputFile *os.File, outputFile *os.File, winnerDocuments map[int]bool) error {
	if _, err := inputFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		fields := util.SplitFields(line, ',')
		if len(fields) != 5 {
			continue
		}
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