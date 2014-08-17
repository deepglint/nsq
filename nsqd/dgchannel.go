package nsqd

import(
	"log"
)


func (c *Channel) router2() {
	//var msgBuf bytes.Buffer
	for msg := range c.incomingMsgChan {
		////////////////////////////
 		//only take the newest msg
 		l := len(c.memoryMsgChan)
 		for i := 0; i < l; i++ {
 			<-c.memoryMsgChan
 		}
 		log.Printf("cleared %d old msg", l)
 		////////////////////////////
		select {
		case c.memoryMsgChan <- msg:

			log.Printf("new msg reveived, len of memoryMsgChan: %d", len(c.memoryMsgChan))
 			log.Printf("                len of incomingMsgChan: %d", len(c.incomingMsgChan))
 			//default:
 			//err := writeMessageToBackend(&msgBuf, msg, c.backend)
 			//if err != nil {
 			//	log.Printf("CHANNEL(%s) ERROR: failed to write message to backend - %s", c.name, err.Error())
 			//	// theres not really much we can do at this point, you're certainly
 			//	// going to lose messages...
 			//}
		}
	}

	log.Printf("DgChannel(%s): closing ... router", c.name)
}