package nsqd

import (
	"bytes"
	//	"conn"
	"encoding/binary"
	"fmt"
	"github.com/deepglint/nsq/internal/protocol"
	//	"io"
	"net"
)

//const defaultMsgSize = 16 * 1024

var separatorLineBytes = []byte("\n")

type udpServer struct {
	ctx      *context
	readdata []byte
}

func (p *udpServer) Handle(clientConn *net.UDPConn) {
	if clientConn == nil {
		return
	}
	//data := make([]byte, p.ctx.nsqd.getOpts().MaxMsgSize+1)
	_, remoteAddr, err := clientConn.ReadFromUDP(p.readdata)
	if err != nil {
		p.ctx.nsqd.logf("ERROR: failed to read UDP msg - %s", err)
		return
	}
	t := bytes.Index(p.readdata, separatorLineBytes)
	if t <= 0 {
		err := protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("invalid message format"))
		clientConn.WriteToUDP([]byte(err.Error()), remoteAddr)
		p.ctx.nsqd.logf("ERROR: failed to parse UDP msg - %s", err)
		return
	}
	paramLine := p.readdata[:t]
	params := bytes.Split(paramLine, separatorBytes)
	content := p.readdata[t+1:]
	response, errExec := p.Exec(params, content)
	if errExec != nil {
		ctx := ""
		if parentErr := errExec.(protocol.ChildErr).Parent(); parentErr != nil {
			ctx = " - " + parentErr.Error()
		}
		p.ctx.nsqd.logf("ERROR: [%s] - %s%s", remoteAddr, errExec, ctx)
		_, sendErr := clientConn.WriteToUDP([]byte(errExec.Error()), remoteAddr)

		if sendErr != nil {
			return
		}

		// errors of type FatalClientErr should forceably close the connection
		if _, ok := errExec.(*protocol.FatalClientErr); ok {
			return
		}
		return
	}

	if response != nil {
		_, err = clientConn.WriteToUDP(response, remoteAddr)
		if err != nil {
			err = fmt.Errorf("failed to send response - %s", err)
			return
		}
	}
}

func (p *udpServer) Exec(params [][]byte, content []byte) ([]byte, error) {
	// if bytes.Equal(params[0], []byte("IDENTIFY")) {
	// 	return p.IDENTIFY(client, params)
	// }
	// err := enforceTLSPolicy(client, p, params[0])
	// if err != nil {
	// 	return nil, err
	// }
	switch {
	// case bytes.Equal(params[0], []byte("FIN")):
	// 	return p.FIN(client, params)
	// case bytes.Equal(params[0], []byte("RDY")):
	// 	return p.RDY(client, params)
	// case bytes.Equal(params[0], []byte("REQ")):
	// 	return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(params, content)
		// case bytes.Equal(params[0], []byte("MPUB")):
		// 	return p.MPUB(client, params)
		// case bytes.Equal(params[0], []byte("DPUB")):
		// 	return p.DPUB(client, params)
		// case bytes.Equal(params[0], []byte("NOP")):
		// 	return p.NOP(client, params)
		// case bytes.Equal(params[0], []byte("TOUCH")):
		// 	return p.TOUCH(client, params)
		// case bytes.Equal(params[0], []byte("SUB")):
		// 	return p.SUB(client, params)
		// case bytes.Equal(params[0], []byte("CLS")):
		// 	return p.CLS(client, params)
		// case bytes.Equal(params[0], []byte("AUTH")):
		// 	return p.AUTH(client, params)
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))

}
func (p *udpServer) PUB(params [][]byte, content []byte) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "PUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("PUB topic name %q is not valid", topicName))
	}

	bodyLen := int32(binary.BigEndian.Uint32(content))

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB invalid message body size %d", bodyLen))
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxMsgSize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB message too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxMsgSize))
	}

	messageBody := content[4 : 4+bodyLen]

	topic := p.ctx.nsqd.GetTopic(topicName)
	msg := NewMessage(<-p.ctx.nsqd.idChan, messageBody)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_PUB_FAILED", "PUB failed "+err.Error())
	}

	return okBytes, nil
}
