// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package utils

import (
	"context"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp" // auth on GKE
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/elastic/go-elasticsearch/v7"
)

func ResolveEsConfigFromk8s(k8sClient client.Client, namespace string, name string) (elasticsearch.Config, error) {
	esCfg := elasticsearch.Config{}

	var service corev1.Service
	svcName := fmt.Sprintf("%s-es-http", name)
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: svcName}, &service)
	if err != nil {
		return elasticsearch.Config{}, err
	}
	if len(service.Spec.Ports) != 1 {
		return elasticsearch.Config{}, fmt.Errorf("not exactly 1 port: %d", len(service.Spec.Ports))
	}
	protocol := service.Spec.Ports[0].Name
	port := service.Spec.Ports[0].Port
	ip := service.Spec.ClusterIP
	ingress := service.Status.LoadBalancer.Ingress
	if len(ingress) == 1 {
		ip = ingress[0].IP
	}
	if ip == "" {
		return elasticsearch.Config{}, fmt.Errorf("service IP not found")
	}

	var secret corev1.Secret
	secretName := fmt.Sprintf("%s-es-elastic-user", name)
	err = k8sClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: secretName}, &secret)
	if err != nil {
		return elasticsearch.Config{}, err
	}
	esCfg.Username = "elastic"
	if password, ok := secret.Data[esCfg.Username]; ok {
		esCfg.Password = string(password)
	} else {
		return elasticsearch.Config{}, fmt.Errorf("no 'elastic' entry in Secret %q", secretName)
	}

	secretName = fmt.Sprintf("%s-es-http-certs-public", name)
	err = k8sClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: secretName}, &secret)
	if err != nil {
		return elasticsearch.Config{}, err
	}
	if caCert, ok := secret.Data["ca.crt"]; ok {
		esCfg.CACert = caCert
	}

	esCfg.Addresses = []string{fmt.Sprintf("%s://%s:%d", protocol, ip, port)}

	return esCfg, nil
}

func NewK8sClient(kubeconfigPath string) (client.Client, error) {
	err := os.Setenv("KUBECONFIG", kubeconfigPath)
	if err != nil {
		return nil, err
	}

	clientCfg, err := config.GetConfig()
	if err != nil {
		return nil, err
	}

	c, err := client.New(clientCfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		return nil, err
	}

	// check the client can connect to k8s
	var secrets corev1.SecretList
	err = c.List(context.Background(), &secrets)
	if err != nil {
		return nil, err
	}

	return c, nil
}
