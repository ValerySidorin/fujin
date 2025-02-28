package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"log"
	"math/big"

	"github.com/quic-go/quic-go"
)

func main() {
	listener, err := quic.ListenAddr(":4242", generateTLSConfig(), nil)
	if err != nil {
		log.Fatal(err)
	}
	for {
		session, err := listener.Accept(context.Background())
		if err != nil {
			log.Println(err)
			continue
		}
		go handleSession(session)
	}
}

func handleSession(session quic.Connection) {
	stream, err := session.AcceptStream(context.Background())
	if err != nil {
		log.Println(err)
		return
	}
	for {
		buf := make([]byte, 512)
		n, err := stream.Read(buf)
		if err != nil {
			log.Println(err)
			return
		}
		buf = buf[:n]
		if string(buf) == "ping" {
			_, err := stream.Write([]byte("pong"))
			if err != nil {
				log.Println(err)
				return
			}
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
	return &tls.Config{Certificates: []tls.Certificate{tlsCert}, InsecureSkipVerify: true}
}
