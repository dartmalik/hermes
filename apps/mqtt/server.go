package mqtt

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/dartali/hermes"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{Subprotocols: []string{"mqtt"}}

type wsEndpoint struct {
	id   uint64
	enc  *Encoder
	dec  *Decoder
	conn *websocket.Conn
	net  *hermes.Hermes
}

func newEndpoint(id uint64, conn *websocket.Conn, net *hermes.Hermes) *wsEndpoint {
	return &wsEndpoint{id: id, conn: conn, net: net, enc: newEncoder(), dec: newDecoder()}
}

func (end *wsEndpoint) Write(msg interface{}) {
	buff, err := end.enc.encode(msg)
	if err != nil {
		fmt.Printf("[ERROR] failed to encode msg: %s\n", err.Error())
		return
	}

	err = end.conn.WriteMessage(websocket.BinaryMessage, buff)
	if err != nil {
		fmt.Printf("[ERROR] failed to write msg: %s\n", err.Error())
	}
}

func (end *wsEndpoint) WriteAndClose(msg interface{}) {
	end.Write(msg)
	end.Close()
}

func (end *wsEndpoint) Close() {
	end.conn.Close()
}

func (end *wsEndpoint) open() error {
	return end.net.Send("", end.cid(), &ClientEndpointCreated{endpoint: end})
}

func (end *wsEndpoint) process() {
	for {
		mt, buff, err := end.conn.ReadMessage()
		if err != nil {
			end.conn.Close()
			break
		}

		if mt != websocket.BinaryMessage {
			break
		}

		msgs, err := end.dec.decode(buff)
		if err != nil {
			break
		}

		for _, m := range msgs {
			end.net.Send("", end.cid(), m)
		}
	}
}

func (end *wsEndpoint) cid() hermes.ReceiverID {
	return clientID(fmt.Sprintf("%d", end.id))
}

type Server struct {
	net    *hermes.Hermes
	connID uint64
}

func NewServer(net *hermes.Hermes) (*Server, error) {
	if net == nil {
		return nil, errors.New("invalid_hermes_instance")
	}

	return &Server{net: net}, nil
}

func (srv *Server) ListenAndServe() error {
	http.HandleFunc("/", srv.onConn)
	return http.ListenAndServe(":8080", nil)
}

func (srv *Server) onConn(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[ERROR] ws upgrade failed: %s\n", err.Error())
		return
	}

	id := atomic.AddUint64(&srv.connID, 1)
	end := newEndpoint(id, conn, srv.net)
	err = end.open()
	if err != nil {
		log.Printf("[ERROR] failed to open endpint: %s\n", err.Error())
		return
	}

	end.process()

	end.Close()
}
