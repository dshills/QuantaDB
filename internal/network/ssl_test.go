package network

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/log"
)

// generateTestCert generates a self-signed certificate for testing
func generateTestCert(certFile, keyFile string) error {
	// Generate private key
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test"},
			Country:      []string{"US"},
		},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour * 24 * 30), // 30 days
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:     []string{"localhost"},
	}

	// Create certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return err
	}

	// Write certificate file
	certOut, err := os.Create(certFile)
	if err != nil {
		return err
	}
	defer certOut.Close()

	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		return err
	}

	// Write key file
	keyOut, err := os.Create(keyFile)
	if err != nil {
		return err
	}
	defer keyOut.Close()

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return err
	}

	return pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})
}

func TestSSLConfiguration(t *testing.T) {
	// Create temporary certificate files
	certFile := "test_cert.pem"
	keyFile := "test_key.pem"
	defer os.Remove(certFile)
	defer os.Remove(keyFile)

	// Generate test certificate
	if err := generateTestCert(certFile, keyFile); err != nil {
		t.Fatalf("Failed to generate test certificate: %v", err)
	}

	// Create server with SSL configuration
	config := DefaultConfig()
	logger := log.Default()
	
	// Mock dependencies would go here, but for this test we'll just verify SSL config
	server := &Server{
		config: config,
		logger: logger,
	}

	// Test SSL configuration
	err := server.ConfigureSSL(certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to configure SSL: %v", err)
	}

	// Verify SSL is enabled
	if !server.config.EnableSSL {
		t.Error("SSL should be enabled after configuration")
	}

	// Verify TLS config is set
	if server.config.TLSConfig == nil {
		t.Error("TLS config should be set")
	}

	// Verify certificate files are recorded
	if server.config.CertFile != certFile {
		t.Errorf("Expected cert file %s, got %s", certFile, server.config.CertFile)
	}

	if server.config.KeyFile != keyFile {
		t.Errorf("Expected key file %s, got %s", keyFile, server.config.KeyFile)
	}
}

func TestSSLConfigurationWithCustomConfig(t *testing.T) {
	// Create server
	config := DefaultConfig()
	logger := log.Default()
	
	server := &Server{
		config: config,
		logger: logger,
	}

	// Create custom TLS config
	customTLSConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		MaxVersion: tls.VersionTLS13,
	}

	// Test SSL configuration with custom config
	server.ConfigureSSLWithConfig(customTLSConfig)

	// Verify SSL is enabled
	if !server.config.EnableSSL {
		t.Error("SSL should be enabled after configuration")
	}

	// Verify TLS config is set correctly
	if server.config.TLSConfig != customTLSConfig {
		t.Error("Custom TLS config should be set")
	}

	if server.config.TLSConfig.MinVersion != tls.VersionTLS12 {
		t.Error("Min TLS version should be TLS 1.2")
	}
}