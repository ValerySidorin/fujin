package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var (
	ErrTLSClientCertsDirNotSpecified = errors.New("client certs dir not specified, while mtls enabled")
	ErrTLSServerCertPathNotSpecified = errors.New("server cert path not specified")
	ErrTLSServerKeyPathNotSpecified  = errors.New("server cert path not specified")
)

type TLSConfig struct {
	Enabled                    bool   `yaml:"enabled"`
	ClientCertsDir             string `yaml:"client_certs_dir"`
	ServerCertPEMPath          string `yaml:"server_cert_pem_path"`
	ServerKeyPEMPath           string `yaml:"server_key_pem_path"`
	RequireAndVerifyClientCert bool   `yaml:"require_and_verify_client_cert"`

	Config *tls.Config `yaml:"-"`
}

func (c *TLSConfig) Parse() error {
	if err := c.validate(); err != nil {
		return fmt.Errorf("validate: %w", err)
	}

	if !c.Enabled {
		return nil
	}

	caCertPool := x509.NewCertPool()

	if c.ClientCertsDir != "" {
		clientCAs, err := os.ReadDir(c.ClientCertsDir)
		if err != nil {
			return fmt.Errorf("read client certs dir: %w", err)
		}

		for _, certEntry := range clientCAs {
			if !certEntry.IsDir() {
				cert, err := os.ReadFile(filepath.Join(c.ClientCertsDir, certEntry.Name()))
				if err != nil {
					return fmt.Errorf("read client cert: %w", err)
				}
				caCertPool.AppendCertsFromPEM(cert)
			}
		}
	}

	cert, err := tls.LoadX509KeyPair(c.ServerCertPEMPath, c.ServerKeyPEMPath)
	if err != nil {
		return fmt.Errorf("load x509 key pair: %w", err)
	}

	clientAuth := tls.NoClientCert
	if c.RequireAndVerifyClientCert {
		clientAuth = tls.RequireAndVerifyClientCert
	}

	c.Config = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caCertPool,
		ClientAuth:   clientAuth,
	}

	return nil
}

func (c *TLSConfig) validate() error {
	if !c.Enabled {
		return nil
	}

	if c.ClientCertsDir == "" && c.RequireAndVerifyClientCert {
		return ErrTLSClientCertsDirNotSpecified
	}

	if c.ServerCertPEMPath == "" {
		return ErrTLSServerCertPathNotSpecified
	}

	if c.ServerKeyPEMPath == "" {
		return ErrTLSServerKeyPathNotSpecified
	}

	return nil
}
