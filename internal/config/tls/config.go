package tls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type TLSConfig struct {
	ClientCertsDir    string `yaml:"client_certs_dir"`
	ServerCertPEMPath string `yaml:"server_cert_pem_path"`
	ServerKeyPEMPath  string `yaml:"server_key_pem_path"`
	MTLSEnabled       bool   `yaml:"mtls_enabled"`
}

func (c *TLSConfig) Validate() error {
	if c.ClientCertsDir == "" && c.MTLSEnabled {
		return errors.New("client certs dir not specified, while mTLS enabled")
	}

	if c.ServerCertPEMPath == "" {
		return errors.New("server cert path not specified")
	}

	if c.ServerKeyPEMPath == "" {
		return errors.New("server key path not specified")
	}

	return nil
}

func (c *TLSConfig) Parse() (*tls.Config, error) {
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("validate: %w", err)
	}

	caCertPool := x509.NewCertPool()

	if c.ClientCertsDir != "" {
		clientCAs, err := os.ReadDir(c.ClientCertsDir)
		if err != nil {
			return nil, fmt.Errorf("read client CAs dir: %w", err)
		}

		for _, certEntry := range clientCAs {
			if !certEntry.IsDir() {
				cert, err := os.ReadFile(filepath.Join(c.ClientCertsDir, certEntry.Name()))
				if err != nil {
					return nil, fmt.Errorf("read client CA: %w", err)
				}
				caCertPool.AppendCertsFromPEM(cert)
			}
		}
	}

	cert, err := tls.LoadX509KeyPair(c.ServerCertPEMPath, c.ServerKeyPEMPath)
	if err != nil {
		return nil, fmt.Errorf("load x509 key pair: %w", err)
	}

	clientAuth := tls.NoClientCert
	if c.MTLSEnabled {
		clientAuth = tls.RequireAndVerifyClientCert
	}

	nextProtos := []string{"fujin"}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caCertPool,
		ClientAuth:   clientAuth,
		NextProtos:   nextProtos,
	}, nil
}
