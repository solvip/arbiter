package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

// PostgreSQL messages
// We only partially do full parsing of messages, as we're not interested
// in the contents of every message.

// ProtocolViolation means that either the client or the server
// broke the PostgreSQL client/server protocol
var ProtocolViolation = errors.New("protocol violation")

// We currently do not support all the PostgreSQL authentcation mechanisms.
var UnsupportedAuthenticationRequest = errors.New("unsupported authentication request")

// Each message must the following interface
type Message interface {
	// Decode a message from r into self
	DecodeFrom(r io.Reader) error

	// Encode the messsge in self into w
	EncodeTo(w io.Writer) error
}

// A MessageBuilder receives a byte and returns the appropriate Message struct.
type MessageBuilder func(byte) (Message, error)

var UnknownMessage = errors.New("unknown message")

// Maps the messages possible from a frontend to it's Message
// err can only be UnknownMessage.
func frontendMessageBuilder(b byte) (ret Message, err error) {
	switch b {
	case 'B':
		ret = new(Bind)
	case 'C':
		ret = new(Close)
	case 'D':
		ret = new(Describe)
	case 'E':
		ret = new(Execute)
	case 'F':
		ret = new(FunctionCall)
	case 'H':
		ret = new(Flush)
	case 'P':
		ret = new(Parse)
	case 'Q':
		ret = new(Query)
	case 'S':
		ret = new(Sync)
	case 'X':
		ret = new(Terminate)
	case 'c':
		ret = new(CopyDone)
	case 'd':
		ret = new(CopyData)
	case 'f':
		ret = new(CopyFail)
	case 'p':
		ret = new(PasswordMessage)
	default:
		err = UnknownMessage
	}

	return
}

// Maps the messages possible from a backend to it's Message
// err can only be UnknownMessage
func backendMessageBuilder(b byte) (ret Message, err error) {
	switch b {
	case 'A':
		ret = new(NotificationResponse)
	case 'C':
		ret = new(CommandComplete)
	case 'D':
		ret = new(DataRow)
	case 'E':
		ret = new(ErrorResponse)
	case 'F':
		ret = new(FunctionCallResponse)
	case 'G':
		ret = new(CopyInResponse)
	case 'H':
		ret = new(CopyOutResponse)
	case 'I':
		ret = new(EmptyQueryResponse)
	case 'K':
		ret = new(BackendKeyData)
	case 'N':
		ret = new(NoticeResponse)
	case 'R':
		ret = new(AuthenticationRequest)
	case 'S':
		ret = new(ParameterStatus)
	case 'T':
		ret = new(RowDescription)
	case 'W':
		ret = new(CopyBothResponse)
	case 'Z':
		ret = new(ReadyForQuery)
	case 'c':
		ret = new(CopyDone)
	case 'd':
		ret = new(CopyData)
	case 'n':
		ret = new(NoData)
	case 's':
		ret = new(PortalSuspended)
	case 't':
		ret = new(ParameterDescription)
	case '1':
		ret = new(ParseComplete)
	case '2':
		ret = new(BindComplete)
	case '3':
		ret = new(CloseComplete)
	default:
		err = UnknownMessage
	}

	return
}

func readInt32(r io.Reader) (n int32, err error) {
	err = binary.Read(r, binary.BigEndian, &n)
	return
}

func readInt16(r io.Reader) (n int16, err error) {
	err = binary.Read(r, binary.BigEndian, &n)
	return
}

func readBody(r io.Reader, len int32) (ret []byte, err error) {
	ret = make([]byte, len-4)
	_, err = io.ReadFull(r, ret)
	return
}

func int32bytes(n int32) (ret []byte) {
	ret = make([]byte, 4)
	binary.BigEndian.PutUint32(ret, uint32(n))
	return
}

func int16bytes(n int16) (ret []byte) {
	ret = make([]byte, 2)
	binary.BigEndian.PutUint16(ret, uint16(n))
	return
}

/* Read a full message.  Must be prefixed by int32 */
func readMessage(r io.Reader) (msglen int32, ret []byte, err error) {
	msglen, err = readInt32(r)
	if err != nil {
		return
	}

	ret, err = readBody(r, msglen)

	return
}

func writeMessage(w io.Writer, msgPrefix byte, fields ...[]byte) (err error) {
	var n int32 = 4
	for _, v := range fields {
		n = n + int32(len(v))
	}

	if _, err = w.Write([]byte{msgPrefix}); err != nil {
		return
	}

	if _, err = w.Write(int32bytes(n)); err != nil {
		return
	}

	for _, v := range fields {
		if _, err = w.Write(v); err != nil {
			return
		}
	}

	return
}

type CopyInResponse []byte

func (c *CopyInResponse) DecodeFrom(r io.Reader) (err error) {
	_, *c, err = readMessage(r)

	return
}

func (c *CopyInResponse) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'G', *c)
}

type NoticeResponse []byte

func (n *NoticeResponse) DecodeFrom(r io.Reader) (err error) {
	_, *n, err = readMessage(r)

	return
}

func (n *NoticeResponse) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'N', *n)
}

type NotificationResponse []byte

func (n *NotificationResponse) DecodeFrom(r io.Reader) (err error) {
	_, *n, err = readMessage(r)

	return
}

func (n *NotificationResponse) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'A', *n)
}

type FunctionCallResponse []byte

func (m *FunctionCallResponse) DecodeFrom(r io.Reader) (err error) {
	_, *m, err = readMessage(r)

	return
}

func (m *FunctionCallResponse) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'V', *m)
}

type CopyOutResponse []byte

func (m *CopyOutResponse) DecodeFrom(r io.Reader) (err error) {
	_, *m, err = readMessage(r)

	return
}

func (m *CopyOutResponse) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'H', *m)
}

type CopyBothResponse []byte

func (m *CopyBothResponse) DecodeFrom(r io.Reader) (err error) {
	_, *m, err = readMessage(r)

	return
}

func (m *CopyBothResponse) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'w', *m)
}

type NoData []byte

func (m *NoData) DecodeFrom(r io.Reader) (err error) {
	_, *m, err = readMessage(r)

	return
}

func (m *NoData) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'n', *m)
}

type PortalSuspended []byte

func (m *PortalSuspended) DecodeFrom(r io.Reader) (err error) {
	_, *m, err = readMessage(r)

	return
}

func (m *PortalSuspended) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 's', *m)
}

type ParameterDescription []byte

func (m *ParameterDescription) DecodeFrom(r io.Reader) (err error) {
	_, *m, err = readMessage(r)

	return
}

func (m *ParameterDescription) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 't', *m)
}

type EmptyQueryResponse struct{}

func (e *EmptyQueryResponse) DecodeFrom(r io.Reader) (err error) {
	msglen, err := readInt32(r)
	if err != nil {
		return
	} else if msglen != 4 {
		return ProtocolViolation
	}

	return
}

func (e *EmptyQueryResponse) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'I')
}

type CommandComplete []byte

func (c *CommandComplete) DecodeFrom(r io.Reader) (err error) {
	_, *c, err = readMessage(r)

	return
}

func (c *CommandComplete) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'C', *c)
}

type DataRow []byte

func (row *DataRow) DecodeFrom(r io.Reader) (err error) {
	_, *row, err = readMessage(r)
	return
}

func (row *DataRow) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'D', *row)
}

type RowDescription []byte

func (desc *RowDescription) DecodeFrom(r io.Reader) (err error) {
	_, *desc, err = readMessage(r)

	return
}

func (desc *RowDescription) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'T', *desc)
}

type ReadyForQuery struct {
	// Current backend transaction status indicator. Possible values are 'I' if idle (not in a transaction block); 'T' if in a transaction block; or 'E' if in a failed transaction block (queries will be rejected until block is ended).
	status byte
}

func (q *ReadyForQuery) DecodeFrom(r io.Reader) (err error) {
	n, msg, err := readMessage(r)
	if err != nil {
		return
	} else if n != 5 {
		return ProtocolViolation
	}

	q.status = msg[len(msg)-1]

	return
}

func (q *ReadyForQuery) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'Z', []byte{q.status})
}

type ParameterStatus struct {
	name  []byte // The name of the parameter this message describes
	value []byte // The value of the parameter
}

func (p *ParameterStatus) DecodeFrom(r io.Reader) (err error) {
	_, msg, err := readMessage(r)
	if err != nil {
		return
	}

	if i := bytes.IndexByte(msg, 0); i < 0 || i+1 > len(msg) {
		return ProtocolViolation
	} else {
		p.name = msg[0 : i+1]
		p.value = msg[i+1 : len(msg)]
	}

	return
}

func (p *ParameterStatus) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'S', p.name, p.value)
}

type BackendKeyData struct {
	pid    int32
	secret int32
}

func (b *BackendKeyData) DecodeFrom(r io.Reader) (err error) {
	len, err := readInt32(r)
	if err != nil {
		return
	} else if len != 12 {
		return ProtocolViolation
	}

	b.pid, err = readInt32(r)
	if err != nil {
		return
	}
	b.secret, err = readInt32(r)

	return
}

func (b *BackendKeyData) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'K', int32bytes(b.pid), int32bytes(b.secret))
}

type Bind struct {
	raw []byte
}

func (b *Bind) DecodeFrom(r io.Reader) (err error) {
	_, b.raw, err = readMessage(r)
	return
}

func (b *Bind) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'B', b.raw)
}

type Parse struct {
	raw []byte
}

func (p *Parse) DecodeFrom(r io.Reader) (err error) {
	_, p.raw, err = readMessage(r)
	return
}

func (p *Parse) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'P', p.raw)
}

type ParseComplete struct {
	raw []byte
}

func (p *ParseComplete) DecodeFrom(r io.Reader) (err error) {
	_, p.raw, err = readMessage(r)
	return
}

func (p *ParseComplete) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, '1', p.raw)
}

type BindComplete struct {
	raw []byte
}

func (p *BindComplete) DecodeFrom(r io.Reader) (err error) {
	_, p.raw, err = readMessage(r)
	return
}

func (p *BindComplete) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, '2', p.raw)
}

type CloseComplete struct {
	raw []byte
}

func (p *CloseComplete) DecodeFrom(r io.Reader) (err error) {
	_, p.raw, err = readMessage(r)
	return
}

func (p *CloseComplete) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, '3', p.raw)
}

type Close struct {
	raw []byte
}

func (p *Close) DecodeFrom(r io.Reader) (err error) {
	_, p.raw, err = readMessage(r)
	return
}

func (p *Close) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'C', p.raw)
}

type Describe struct {
	raw []byte
}

func (p *Describe) DecodeFrom(r io.Reader) (err error) {
	_, p.raw, err = readMessage(r)
	return
}

func (p *Describe) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'D', p.raw)
}

type Execute struct {
	raw []byte
}

func (p *Execute) DecodeFrom(r io.Reader) (err error) {
	_, p.raw, err = readMessage(r)
	return
}

func (p *Execute) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'E', p.raw)
}

type FunctionCall struct {
	raw []byte
}

func (p *FunctionCall) DecodeFrom(r io.Reader) (err error) {
	_, p.raw, err = readMessage(r)
	return
}

func (p *FunctionCall) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'F', p.raw)
}

type Flush struct {
	raw []byte
}

func (p *Flush) DecodeFrom(r io.Reader) (err error) {
	_, p.raw, err = readMessage(r)
	return
}

func (p *Flush) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'H', p.raw)
}

type CopyFail struct {
	raw []byte
}

func (p *CopyFail) DecodeFrom(r io.Reader) (err error) {
	_, p.raw, err = readMessage(r)
	return
}

func (p *CopyFail) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'P', p.raw)
}

type Query []byte

func (q *Query) DecodeFrom(r io.Reader) (err error) {
	_, *q, err = readMessage(r)
	return
}

func (q *Query) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'Q', *q)
}

type Terminate struct{}

func (e *Terminate) DecodeFrom(r io.Reader) (err error) {
	n, err := readInt32(r)
	if err != nil {
		return
	} else if n != 4 {
		return ProtocolViolation
	}

	return
}

func (e *Terminate) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'X')
}

type Sync struct{}

func (e *Sync) DecodeFrom(r io.Reader) (err error) {
	msglen, err := readInt32(r)
	if err != nil {
		return
	} else if msglen != 4 {
		err = ProtocolViolation
	}

	return
}

func (e *Sync) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'S')
}

type ErrorResponse struct {
	fields map[byte]string
}

func (e *ErrorResponse) DecodeFrom(r io.Reader) (err error) {
	_, msg, err := readMessage(r)
	if err != nil {
		return
	}

	e.fields = make(map[byte]string)

	for _, item := range bytes.Split(msg, []byte{0}) {
		if len(item) < 2 {
			break
		}

		e.fields[item[0]] = string(item[1:])
	}

	return
}

func (e *ErrorResponse) EncodeTo(w io.Writer) (err error) {
	fields := make([]byte, 0)
	for k, v := range e.fields {
		fields = append(fields, k)
		fields = append(fields, []byte(v)...)
		fields = append(fields, 0)
	}
	fields = append(fields, 0)

	msglen := make([]byte, 4)
	binary.BigEndian.PutUint32(msglen, uint32(len(msglen)+len(fields)))

	return writeMessage(w, 'E', fields)
}

type CopyDone struct{}

func (c *CopyDone) DecodeFrom(r io.Reader) (err error) {
	n, err := readInt32(r)
	if err != nil {
		return
	} else if n != 4 {
		return ProtocolViolation
	}

	return
}

func (c *CopyDone) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'c')
}

type CopyData struct {
	raw []byte // No interest in contents
}

func (cd *CopyData) DecodeFrom(r io.Reader) (err error) {
	_, cd.raw, err = readMessage(r)
	return
}

func (cd *CopyData) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'd', cd.raw)
}

type PasswordMessage struct {
	password []byte
}

func (p *PasswordMessage) DecodeFrom(r io.Reader) (err error) {
	_, p.password, err = readMessage(r)

	return
}

func (p *PasswordMessage) EncodeTo(w io.Writer) (err error) {
	return writeMessage(w, 'p', p.password)
}

type AuthenticationRequest struct {
	Type AuthenticationType
	salt []byte
}

type AuthenticationType int32

const (
	OK                AuthenticationType = 0
	KerberosV5                           = 2
	CleartextPassword                    = 3
	MD5Password                          = 5
	SCMCredential                        = 6
	GSS                                  = 7
	GSSContinue                          = 8
	SSPI                                 = 9
)

func (ar *AuthenticationRequest) EncodeTo(w io.Writer) (err error) {
	switch ar.Type {
	case OK, CleartextPassword, MD5Password:
		break
	default:
		return UnsupportedAuthenticationRequest
	}

	msgType := make([]byte, 4)
	binary.BigEndian.PutUint32(msgType, uint32(ar.Type))

	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(length)+len(msgType)+len(ar.salt)))

	return writeMessage(w, 'R', msgType, ar.salt)
}

// Decode into an AuthenticationRequest.
// If the message type is incorrect, ProtocolViolation is returned.
// If the authentication type is not supported, UnsupportedAuthenticationRequest is returned.
func (ar *AuthenticationRequest) DecodeFrom(r io.Reader) (err error) {
	msglen, err := readInt32(r)
	if err != nil {
		return
	} else if msglen < 8 {
		return ProtocolViolation
	}

	if n, err := readInt32(r); err != nil {
		return err
	} else {
		ar.Type = AuthenticationType(n)
	}

	switch {
	case ar.Type == OK && msglen == 8:
		break

	case ar.Type == CleartextPassword && msglen == 8:
		break

	case ar.Type == MD5Password && msglen == 12:
		ar.salt = make([]byte, 4)
		if _, err = io.ReadFull(r, ar.salt); err != nil {
			return err
		}

	default:
		return UnsupportedAuthenticationRequest
	}

	return
}

type StartupMessage struct {
	version    int32
	parameters map[string]string
}

func (s *StartupMessage) DecodeFrom(r io.Reader) (err error) {
	totalLen, err := readInt32(r)
	if err != nil {
		return err
	}

	if err = binary.Read(r, binary.BigEndian, &s.version); err != nil {
		return err
	}
	s.parameters = make(map[string]string)

	/* Subtract 8, 4 for message length, 4 for version */
	rawParams := make([]byte, totalLen-8)
	if _, err = io.ReadFull(r, rawParams); err != nil {
		return err
	}

	params := bytes.Split(rawParams[0:len(rawParams)], []byte{0})

	var key, val string
	for i, item := range params {
		/* At the end */
		if len(item) == 0 {
			break
		}

		if i%2 == 0 {
			key = string(item)
		} else {
			val = string(item)
			s.parameters[key] = val
		}
	}

	return nil
}

func (s *StartupMessage) MajorVersion() int {
	return int(s.version >> 16)
}

func (s *StartupMessage) MinorVersion() int {
	return int(s.version & 0xFFFF)
}

func (s *StartupMessage) EncodeTo(w io.Writer) (err error) {
	// The binary format of the StartupMessage is:
	// length 4 bytes
	// version 4 bytes
	// key val pairs, every field null terminated.
	// null terminator 1 byte

	/* 4 for version, 4 for length */
	version := int32bytes(s.version)

	params := make([]byte, 0)
	for k, v := range s.parameters {
		params = append(params, k...)
		params = append(params, 0)

		params = append(params, v...)
		params = append(params, 0)
	}
	params = append(params, 0)

	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(version)+len(length)+len(params)))

	err = WriteSlices(w, length, version, params)

	return err
}

func WriteSlices(w io.Writer, slices ...[]byte) (err error) {
	for _, slice := range slices {
		if _, err = w.Write(slice); err != nil {
			return
		}
	}

	return
}
