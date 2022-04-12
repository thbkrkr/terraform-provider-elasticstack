// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package utils

import (
	"context"
	"io"
	"log"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/elastic/go-elasticsearch/v7"
)

func Test_ResolveEsConfigFromk8s(t *testing.T) {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns-a",
			Name:      "cluster-a-es-http",
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name: "http",
					Port: 9200,
				},
			},
			ClusterIP: "127.0.0.1",
		},
	}
	userSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns-a",
			Name:      "cluster-a-es-elastic-user",
		},
		Data: map[string][]byte{
			"elastic": []byte("REDACTED"),
		},
	}
	invalidUserSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns-a",
			Name:      "cluster-a-es-elastic-user",
		},
		Data: map[string][]byte{
			"badEntry": []byte("REDACTED"),
		},
	}
	certsSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns-a",
			Name:      "cluster-a-es-http-certs-public",
		},
	}

	tests := []struct {
		name     string
		initObjs []runtime.Object
		wantErr  bool
	}{
		{
			name:     "happy path",
			initObjs: []runtime.Object{svc, userSecret, certsSecret},
			wantErr:  false,
		},
		{
			name:     "missing service",
			initObjs: []runtime.Object{userSecret},
			wantErr:  true,
		},
		{
			name:     "missing user secret",
			initObjs: []runtime.Object{svc},
			wantErr:  true,
		},
		{
			name:     "invalid user secret",
			initObjs: []runtime.Object{svc, invalidUserSecret},
			wantErr:  true,
		},
	}

	fakeEs := startFakeElasticsearch()
	defer fakeEs.stop()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8sClient := fake.NewClientBuilder().WithRuntimeObjects(tt.initObjs...).WithScheme(scheme.Scheme).Build()
			esConfig, err := ResolveEsConfigFromk8s(k8sClient, "ns-a", "cluster-a")
			if (err != nil) != tt.wantErr {
				t.Errorf("ResolveEsConfigFromk8s() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				esClient, err := elasticsearch.NewClient(esConfig)
				require.NoError(t, err)

				res, err := esClient.Info()
				require.NoError(t, err)
				require.Equal(t, 200, res.StatusCode)
			}
		})
	}
}

type fakeElasticsearch struct {
	*http.Server
	exitDone *sync.WaitGroup
}

func startFakeElasticsearch() fakeElasticsearch {
	es := fakeElasticsearch{
		Server:   &http.Server{Addr: ":9200"},
		exitDone: &sync.WaitGroup{},
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		_, _ = io.WriteString(w, `{
  "name" : "cluster-a-es-master-0",
  "cluster_name" : "cluster-a",
  "cluster_uuid" : "wv4925kBQAarma0wiJvJFA",
  "version" : {
    "number" : "7.16.3",
    "build_flavor" : "default",
    "build_type" : "docker",
    "build_hash" : "4e6e4eab2297e949ec994e688dad46290d018022",
    "build_date" : "2022-01-06T23:43:02.825887787Z",
    "build_snapshot" : false,
    "lucene_version" : "8.10.1",
    "minimum_wire_compatibility_version" : "6.8.0",
    "minimum_index_compatibility_version" : "6.0.0-beta1"
  },
  "tagline" : "You Know, for Search"
}
`)
	})

	es.exitDone.Add(1)
	go func() {
		defer es.exitDone.Done()
		if err := es.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("Fail to start fake Elasticsearch server: %v", err)
		}
	}()

	return es
}

func (es fakeElasticsearch) stop() {
	if err := es.Shutdown(context.Background()); err != nil {
		log.Fatalf("Fail to stop fake Elasticsearch server: %v", err)
	}
	es.exitDone.Wait()
}
