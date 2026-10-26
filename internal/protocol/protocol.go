// Package protocol : le contrat entre le server et le client.
//
// Règle d'équipe : toute modification de ce fichier = un commit à part
// (ex. "feat(protocol): add produce op") + on prévient l'autre.
//
// Format : chaque Request / Response / Delivery = une ligne JSON terminée par '\n'.
package protocol

import (
	"fmt"
	"regexp"
)

// ---- 1. Limites (sujet VI.4) ----
const (
	MaxPayload = 1024 // key + value max, en octets
	MaxLine    = 4096
	MaxOffset  = 1<<31 - 1 // int32 max limit for offset (incrementing 32-bit integer)
)

// ---- 2. Codes de sortie (sujet VIII.4) ----
// Priorité si plusieurs erreurs : 1 > 3 > 2
const (
	ExitOK      = 0
	ExitGeneral = 1 // mauvais arguments, nom invalide, IPC identifier invalide
	ExitTopic   = 2 // topic / client inexistant ou en double
	ExitIPC     = 3 // communication coupée avec le server
)

// ---- 3. Opérations (sujet VIII.1) ----
// On commence par create seulement. On ajoute les autres au fur et à mesure.
const (
	OpCreate    = "create"
	OpList      = "list"
	OpInfo      = "info"
	OpProduce   = "produce"
	OpSubscribe = "subscribe"
	OpCheck     = "check"
)

// ---- 4. Types de livraison (server -> consumer) ----
const (
	DeliveryMessage  = "message"  // un vrai message
	DeliveryShutdown = "shutdown" // le server s'arrête : le consumer doit sortir avec 0 (sujet VI.8)
)

// Request : client -> server (écrit dans le FIFO principal du server).
type Request struct {
	Op     string `json:"op"`
	Reply  string `json:"reply,omitempty"` // chemin du FIFO de réponse du client
	Topic  string `json:"topic,omitempty"`
	Client string `json:"client,omitempty"` // subscriber name
	Key    []byte `json:"key,omitempty"`    // produce
	Value  []byte `json:"value,omitempty"`  // produce
}

// Response : server -> client (écrit dans le FIFO de réponse du client).
type Response struct {
	Code   int         `json:"code"`             // 0 = OK, sinon = code de sortie du client
	Error  string      `json:"error,omitempty"`  // le client l'affiche sur stderr
	Topics []string    `json:"topics,omitempty"` // liste ordonnee des topics crees sur le serveur
	Info   *ClientInfo `json:"info,omitempty"`   // filled and given when 'info' command treated successfully
}

// ClientInfo : sortie de la commande info. Champs imposés par le sujet (VIII.1 / VIII.3).
type ClientInfo struct {
	Client string `json:"client"`
	Topic  string `json:"topic"`
	Offset int    `json:"offset"`
	Prefix string `json:"prefix"`
	IPC    string `json:"ipc"`
}

// Delivery : server -> consumer. Un message = key + value + offset (sujet VI.4).
type Delivery struct {
	Type   string `json:"type"` // DeliveryMessage ou DeliveryShutdown
	Offset int32  `json:"offset"`
	Key    []byte `json:"key,omitempty"`
	Value  []byte `json:"value,omitempty"`
}

// Noms de topic et de client : ^[a-zA-Z0-9_.-]{1,32}$ (sujet VI.5)
var namePattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,32}$`)

func ValidName(name string) bool {
	return namePattern.MatchString(name)
}

// ---- Chemins des FIFO ----
// FIFO principal du server : /tmp/treenity.server.<pid_server>
// FIFO de réponse          : /tmp/treenity.server.<pid_server>:reply:<pid_client>

func ServerPath(serverPID int) string {
	return fmt.Sprintf("/tmp/treenity.server.%d", serverPID)
}

func ReplyPath(endpoint string, clientPID int) string {
	return fmt.Sprintf("%s:reply:%d", endpoint, clientPID)
}
