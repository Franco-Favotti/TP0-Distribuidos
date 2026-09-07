package protocol

import (
	"encoding/binary"
	"io"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const (
	MsgBetBatch byte = 1
	MsgBatchAck byte = 2
	MsgDone     byte = 3
	MsgWinners  byte = 4
)

type Bet struct {
	FirstName string
	LastName  string
	Document  string
	Birthdate string
	Number    string
}

//Concatena agencyId + apuestas separadas por salto de linea
func Encode_bet_batch(agencyId string, bets []Bet) []byte {
	payload := agencyId + "|"
	for _, bet := range bets {
		payload += bet.FirstName + "," + bet.LastName + "," + bet.Document + "," +
			bet.Birthdate + "," + bet.Number + "\n"
	}
	return []byte(payload)
}

//Arma el Header de 5 bytes (tipo + longitud) y lo manda junto con el payload
func Write_message(w io.Writer, msgType byte, payload []byte) error {
	header := make([]byte, 5)
	header[0] = msgType
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	return safe_socket.SendAll(w, append(header, payload...))
}

//Lee el Header de 5 bytes, extrae tipo y longitud, y lee el payload.
func Read_message(r io.Reader) (byte, []byte, error) {
	header, err := safe_socket.RecvAll(r, 5)
	if err != nil {
		return 0, nil, err
	}

	msgType := header[0]
	length := binary.BigEndian.Uint32(header[1:])
	payload := []byte{}	
	if length > 0{
		payload, err = safe_socket.RecvAll(r, int(length))
		if err != nil{
			return 0, nil, err
		}
	}

	return msgType, payload, nil
	
}

