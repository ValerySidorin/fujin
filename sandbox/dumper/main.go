package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/big"
	"os/signal"
	"syscall"
	"time"

	"github.com/ValerySidorin/fujin/server/fujin/proto/request"
	"github.com/ValerySidorin/fujin/server/fujin/proto/response"
	"github.com/quic-go/quic-go"
)

var (
	addr = "localhost:4848"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	conn, err := setup(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// go produceLoopTx(ctx, conn)

	// if err := produce(ctx, conn); err != nil {
	// 	log.Fatal(err)
	// }

	if err := produceLoop(ctx, conn); err != nil {
		log.Fatal(err)
	}

	// if err := produceByBytes(ctx, conn); err != nil {
	// 	log.Fatal(err)
	// }

	// if err := produceTxByBytes(ctx, conn); err != nil {
	// 	log.Fatal(err)
	// }

	// if err := produceTx(ctx, conn); err != nil {
	// 	log.Fatal(err)
	// }

	// if err := consume(ctx, conn); err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println("consuming")
	//time.Sleep(10 * time.Second)

	// if err := produce(ctx, conn); err != nil {
	// 	log.Fatal(err)
	// }

	// if err := subscribe(ctx, "sub", conn); err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println("subscribed")

	// if err := subscribeByBytes(ctx, "sub", conn); err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println("subscribed")

	// time.Sleep(10 * time.Second)

	// time.Sleep(5 * time.Second)

	<-ctx.Done()
	conn.CloseWithError(0x0, "")
}

func setup(ctx context.Context) (quic.Connection, error) {
	conn, err := quic.DialAddr(ctx, addr, generateTLSConfig(), nil)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			str, err := conn.AcceptStream(ctx)
			if err != nil {
				log.Fatal("accept stream: ", err)
			}
			fmt.Printf("accepted stream: id: %d\n", str.StreamID())

			go handlePing(str)
		}
	}()

	return conn, nil
}

func subscribe(ctx context.Context, sub string, conn quic.Connection) error {
	// req := make([]byte, 0, 9+len(sub))
	// req = append(req, byte(request.ConnectSubscriberOpCode))
	// req = req[:9]
	// binary.BigEndian.PutUint32(req[1:5], 0)
	// binary.BigEndian.PutUint32(req[5:9], uint32(len(sub)))
	// req = append(req, []byte(sub)...)

	req := make([]byte, 0, 5+len(sub))
	req = append(req, byte(request.OP_CODE_CONNECT_SUBSCRIBER))
	req = req[:5]
	binary.BigEndian.PutUint32(req[1:5], uint32(len(sub)))
	req = append(req, []byte(sub)...)

	str, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	fmt.Println("subscribe: opened stream")

	if _, err := str.Write(req); err != nil {
		return err
	}
	fmt.Println("subscribe: write req")

	go read(str, "subscribe")

	// ackReq := []byte{
	// 	byte(request.OP_CODE_ACK),
	// 	0, 0, 0, 1,
	// 	1, 0, 0, 0, 0, 0, 0, 1,
	// }

	// if _, err := str.Write(ackReq); err != nil {
	// 	return err
	// }

	// time.Sleep(10 * time.Second)
	// dReq := []byte{
	// 	byte(request.DisconnectOpCode), // cmd
	// }

	// if _, err := str.Write(dReq); err != nil {
	// 	return err
	// }

	// str.Close()
	return nil
}

func subscribeByBytes(ctx context.Context, sub string, conn quic.Connection) error {
	req := make([]byte, 0, 5+len(sub))
	req = append(req, byte(request.OP_CODE_CONNECT_SUBSCRIBER))
	req = req[:5]
	binary.BigEndian.PutUint32(req[1:5], uint32(len(sub)))
	req = append(req, []byte(sub)...)

	str, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	fmt.Println("subscribe: opened stream")

	for _, b := range req {
		if _, err := str.Write([]byte{b}); err != nil {
			return err
		}
		fmt.Println("subscribe: written:", b)
		time.Sleep(1 * time.Second)
	}
	fmt.Println("subscribed")

	go read(str, "subscribe")

	return nil
}

func produceByBytes(ctx context.Context, conn quic.Connection) error {
	req := []byte{
		byte(request.OP_CODE_CONNECT_WRITER),
		0, 0, 0, 0, // producer id is optional (for transactions)
		byte(request.OP_CODE_WRITE),
		0, 1, 1, 1, // request id
		0, 0, 0, 3, // pub len
		112, 117, 98, // pub val
		0, 0, 0, 5, // msg len
		104, 101, 108, 108, 111, // msg
		byte(request.OP_CODE_WRITE),
		0, 1, 1, 1, // request id
		0, 0, 0, 3, // pub len
		112, 117, 98, // pub val
		0, 0, 0, 5, // msg len
		104, 101, 108, 108, 111, // msg
		byte(request.OP_CODE_WRITE),
		0, 1, 1, 1, // request id
		0, 0, 0, 3, // pub len
		112, 117, 98, // pub val
		0, 0, 0, 5, // msg len
		104, 101, 108, 108, 111, // msg
		byte(request.OP_CODE_WRITE),
		0, 1, 1, 1, // request id
		0, 0, 0, 3, // pub len
		112, 117, 98, // pub val
		0, 0, 0, 5, // msg len
		104, 101, 108, 108, 111, // msg
		byte(request.OP_CODE_WRITE),
		0, 1, 1, 1, // request id
		0, 0, 0, 3, // pub len
		112, 117, 98, // pub val
		0, 0, 0, 5, // msg len
		104, 101, 108, 108, 111, // msg
	}

	str, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	fmt.Println("produce: opened stream")

	go read(str, "produce")

	for _, b := range req {
		if _, err := str.Write([]byte{b}); err != nil {
			return err
		}
		fmt.Println("written:", b)
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("produce: write req")
	// time.Sleep(10 * time.Millisecond)

	req2 := []byte{
		byte(request.OP_CODE_DISCONNECT),
	}
	if _, err := str.Write(req2); err != nil {
		return err
	}
	str.Close()

	return nil
}

func produceTxByBytes(ctx context.Context, conn quic.Connection) error {
	req := []byte{
		byte(request.OP_CODE_CONNECT_WRITER),
		0, 0, 0, 3, // producer id len
		112, 117, 98, // // producer id
		byte(request.OP_CODE_TX_BEGIN),
		0, 0, 1, 0, // request id
		byte(request.OP_CODE_WRITE),
		1, 1, 1, 1, // request id
		0, 0, 0, 3, // pub len
		112, 117, 98, // pub val
		0, 0, 0, 5, // msg len
		104, 101, 108, 108, 111, // msg
		byte(request.OP_CODE_TX_COMMIT),
		0, 0, 1, 1, // request id
		byte(request.OP_CODE_WRITE),
		1, 1, 0, 0, // request id
		0, 0, 0, 3, // pub len
		112, 117, 98, // pub val
		0, 0, 0, 5, // msg len
		104, 101, 108, 108, 111, // msg
	}

	str, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	fmt.Println("produce: opened stream")

	go read(str, "produce")

	for _, b := range req {
		if _, err := str.Write([]byte{b}); err != nil {
			return err
		}
		fmt.Println("written:", b)
		time.Sleep(1000 * time.Millisecond)
	}

	fmt.Println("produce: write req")
	// time.Sleep(10 * time.Millisecond)

	req2 := []byte{
		byte(request.OP_CODE_DISCONNECT),
	}
	if _, err := str.Write(req2); err != nil {
		return err
	}
	str.Close()

	return nil
}

func produce(ctx context.Context, conn quic.Connection) error {
	req := []byte{
		byte(request.OP_CODE_CONNECT_WRITER),
		0, 0, 0, 0, // producer id is optional (for transactions)
		byte(request.OP_CODE_WRITE),
		0, 0, 0, 0, // request id
		0, 0, 0, 3, // pub len
		112, 117, 98, // pub val
		0, 0, 0, 2, // msg len
		98, 98, // msg
		byte(request.OP_CODE_WRITE),
		0, 0, 0, 0, // request id
		0, 0, 0, 3, // pub len
		112, 117, 98, // pub val
		0, 0, 0, 2, // msg len
		98, 98, // msg
	}

	str, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	fmt.Println("produce: opened stream")

	if _, err := str.Write(req); err != nil {
		return err
	}

	go read(str, "produce")

	fmt.Println("produce: write req")
	time.Sleep(10 * time.Millisecond)

	req2 := []byte{
		byte(request.OP_CODE_DISCONNECT),
	}
	if _, err := str.Write(req2); err != nil {
		return err
	}
	str.Close()

	return nil
}

func produceTx(ctx context.Context, conn quic.Connection) error {
	req := []byte{
		byte(request.OP_CODE_CONNECT_WRITER),
		0, 0, 0, 3, // producer id len
		112, 117, 98, // // producer id
		byte(request.OP_CODE_TX_BEGIN),
		0, 0, 1, 0, // request id
		byte(request.OP_CODE_WRITE),
		1, 1, 1, 1, // request id
		0, 0, 0, 3, // pub len
		112, 117, 98, // pub val
		0, 0, 0, 5, // msg len
		104, 101, 108, 108, 111, // msg
		byte(request.OP_CODE_TX_COMMIT),
		0, 0, 1, 1, // request id
		byte(request.OP_CODE_WRITE),
		1, 1, 0, 0, // request id
		0, 0, 0, 3, // pub len
		112, 117, 98, // pub val
		0, 0, 0, 5, // msg len
		104, 101, 108, 108, 111, // msg
	}

	str, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	fmt.Println("produce tx: opened stream")

	if _, err := str.Write(req); err != nil {
		return err
	}

	go read(str, "produce tx")

	fmt.Println("produce tx: write req")
	time.Sleep(1 * time.Second)

	req2 := []byte{
		byte(request.OP_CODE_DISCONNECT),
	}
	if _, err := str.Write(req2); err != nil {
		return err
	}
	str.Close()

	return nil
}

func consume(ctx context.Context, conn quic.Connection) error {
	req := []byte{
		byte(request.OP_CODE_CONNECT_CONSUMER), // cmd
		0, 0, 0, 0,                             // request id
		0, 0, 0, 2, // num msgs to consume in batch
		0, 0, 0, 3, // sub len
		115, 117, 98, // sub val
	}

	str, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	fmt.Println("consume: opened stream")

	if _, err := str.Write(req); err != nil {
		return err
	}

	tReq := []byte{
		byte(request.OP_CODE_FETCH), // cmd
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if _, err := str.Write(tReq); err != nil {
					log.Fatal(err)
				}
				time.Sleep(1 * time.Second)
			}
		}
	}()

	fmt.Println("consume: write req")

	go read(str, "consume")

	time.Sleep(10 * time.Second)
	dReq := []byte{
		byte(request.OP_CODE_DISCONNECT), // cmd
	}

	if _, err := str.Write(dReq); err != nil {
		return err
	}

	str.Close()
	return nil
}

func produceLoop(ctx context.Context, conn quic.Connection) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := produce(ctx, conn); err != nil {
				return err
			}
			time.Sleep(1000 * time.Millisecond)
		}
	}
}

func produceLoopTx(ctx context.Context, conn quic.Connection) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := produceTx(ctx, conn); err != nil {
				return err
			}
			time.Sleep(2000 * time.Millisecond)
		}
	}
}

func generateTLSConfig() *tls.Config {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	cert, _ := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	tlsCert := tls.Certificate{
		Certificate: [][]byte{cert},
		PrivateKey:  key,
	}
	return &tls.Config{Certificates: []tls.Certificate{tlsCert}, InsecureSkipVerify: true, NextProtos: []string{"fujin"}}
}

func read(str quic.ReceiveStream, prefix string) {
	buf := make([]byte, 32768)

	for {
		n, err := str.Read(buf)
		if err == io.EOF {
			if n != 0 {
				fmt.Printf("%s: read stream: id: %d data: %v\n", prefix, str.StreamID(), buf[:n])
			}
			fmt.Printf("%s: closed stream: id: %d\n", prefix, str.StreamID())
			return
		}
		if err != nil {
			log.Fatalf("%s: read: %s", prefix, err)
		}

		// fmt.Println("read")

		// comment this for maximum throughput
		fmt.Printf("%s: read stream: id: %d data: %v\n", prefix, str.StreamID(), buf[:n])
	}
}

func handlePing(str quic.Stream) {
	defer str.Close()
	var pingBuf [1]byte

	n, err := str.Read(pingBuf[:])
	if err == io.EOF {
		if n != 0 {
			fmt.Printf("ping: read stream: id: %d data: %v\n", str.StreamID(), pingBuf[:n])
		}
		fmt.Printf("ping: closed stream: id: %d\n", str.StreamID())
		pingBuf[0] = byte(response.RESP_CODE_PONG)
		if _, err := str.Write(pingBuf[:]); err != nil {
			log.Fatal("ping: write pong: ", err)
		}
		return
	}
	if err != nil {
		log.Fatal("ping: read", err)
	}
}
