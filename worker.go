package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type PayloadGenerator func(i int) string

func defaultPayloadGen() PayloadGenerator {
	return func(i int) string {
		return fmt.Sprintf("this is msg #%d!", i)
	}
}

func constantPayloadGenerator(payload string) PayloadGenerator {
	return func(i int) string {
		return payload
	}
}

type Worker struct {
	WorkerId             int
	BrokerUrl            string
	Username             string
	Password             string
	SkipTLSVerification  bool
	NumberOfMessages     int
	PayloadGenerator     PayloadGenerator
	Timeout              time.Duration
	Retained             bool
	PublisherQoS         byte
	CA                   []byte
	Cert                 []byte
	Key                  []byte
	PauseBetweenMessages time.Duration
}

func setSkipTLS(o *mqtt.ClientOptions) {
	oldTLSCfg := o.TLSConfig
	oldTLSCfg.InsecureSkipVerify = true
	o.SetTLSConfig(oldTLSCfg)
}

func NewTLSConfig(ca, certificate, privkey []byte) (*tls.Config, error) {
	// Import trusted certificates from CA
	certpool := x509.NewCertPool()
	ok := certpool.AppendCertsFromPEM(ca)

	if !ok {
		return nil, fmt.Errorf("CA is invalid")
	}

	// Import client certificate/key pair
	cert, err := tls.X509KeyPair(certificate, privkey)
	if err != nil {
		return nil, err
	}

	// Create tls.Config with desired tls properties
	return &tls.Config{
		// RootCAs = certs used to verify server cert.
		RootCAs: certpool,
		// ClientAuth = whether to request cert from server.
		// Since the server is set up for SSL, this happens
		// anyways.
		ClientAuth: tls.NoClientCert,
		// ClientCAs = certs used to validate client cert.
		ClientCAs: nil,
		// InsecureSkipVerify = verify that cert contents
		// match server. IP matches what is in cert etc.
		InsecureSkipVerify: false,
		// Certificates = list of certs client sends to server.
		Certificates: []tls.Certificate{cert},
	}, nil
}

func (w *Worker) Run(ctx context.Context) {
	verboseLogger.Printf("[%d] initializing\n", w.WorkerId)

	cid := w.WorkerId
	t := randomSource.Int31()

	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	var id_num = fmt.Sprintf("%014d", w.WorkerId)
	// var id_num500 = fmt.Sprintf("%014d", w.WorkerId+500)

	topicName := fmt.Sprintf(topicNameTemplate, hostname, id_num, t)
	// publisherClientId := fmt.Sprintf(publisherClientIdTemplate, hostname, w.WorkerId, t)

	publisherClientId := fmt.Sprintf(publisherClientIdTemplate, id_num)

	verboseLogger.Printf("[%d] topic=%s publisherClientId=%s\n", cid, topicName, publisherClientId)

	publisherOptions := mqtt.NewClientOptions().SetClientID(publisherClientId).SetUsername(w.Username).SetPassword(w.Password).AddBroker(w.BrokerUrl)

	if w.SkipTLSVerification {
		setSkipTLS(publisherOptions)
	}

	if len(w.CA) > 0 || len(w.Key) > 0 {
		tlsConfig, err := NewTLSConfig(w.CA, w.Cert, w.Key)
		if err != nil {
			panic(err)
		}
		publisherOptions.SetTLSConfig(tlsConfig)
	}

	publisher := mqtt.NewClient(publisherOptions)

	verboseLogger.Printf("[%d] connecting publisher\n", w.WorkerId)
	if token := publisher.Connect(); token.WaitTimeout(w.Timeout) && token.Error() != nil {
		resultChan <- Result{
			WorkerId:     w.WorkerId,
			Event:        ConnectFailedEvent,
			Error:        true,
			ErrorMessage: token.Error(),
		}
		return
	}

	publishedCount := 0

	t0 := time.Now()
	for i := 0; i < w.NumberOfMessages; i++ {
		verboseLogger.Printf("[%d] publish\n", w.WorkerId)
		text := w.PayloadGenerator(i)
		token := publisher.Publish(topicName, w.PublisherQoS, w.Retained, text)
		publishedCount++
		token.WaitTimeout(w.Timeout)
		time.Sleep(w.PauseBetweenMessages)
	}
	publisher.Disconnect(5)

	publishTime := time.Since(t0)
	verboseLogger.Printf("[%d] all messages published\n", w.WorkerId)
	verboseLogger.Printf("[%v] publishTime\n", publishTime)

				resultChan <- Result{
					WorkerId:          w.WorkerId,
					Event:             CompletedEvent,
					PublishTime:       publishTime,
					MessagesPublished: publishedCount,
				}

	verboseLogger.Printf("[%d] worker finished\n", w.WorkerId)
}
