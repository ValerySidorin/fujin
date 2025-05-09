package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/ValerySidorin/fujin/internal/connector/cerr"
)

type ClientTLSConfig struct {
	ClientCertPath     string `yaml:"client_cert_path"`
	ClientKeyPath      string `yaml:"client_key_path"`
	ServerCertPath     string `yaml:"server_cert_path"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
	Mutual             bool   `yaml:"-"`
}

func (c *ClientTLSConfig) Validate() error {
	if c.ServerCertPath == "" {
		return cerr.ValidationErr("server cert path not provided")
	}

	if c.ClientCertPath != "" || c.ClientKeyPath != "" {
		if c.ClientCertPath == "" {
			return cerr.ValidationErr("client cert path not provided")
		}

		if c.ClientKeyPath == "" {
			return cerr.ValidationErr("client key path not provided")
		}

		c.Mutual = true
	}

	return nil
}

func (c *ClientTLSConfig) Parse() (*tls.Config, error) {
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("validate: %w", err)
	}

	caCert, err := os.ReadFile(c.ServerCertPath)
	if err != nil {
		return nil, fmt.Errorf("read server cert file: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("append certs from PEM")
	}

	tlsConf := &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: c.InsecureSkipVerify,
	}

	if c.Mutual {
		cert, err := tls.LoadX509KeyPair(c.ClientCertPath, c.ClientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load cert from key pair: %w", err)
		}

		tlsConf.Certificates = []tls.Certificate{cert}
	}

	return tlsConf, nil
}
