package p115

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"math/big"
)

// Standard M115 RSA-1024 + XOR cipher used by 115 Pro API (e.g. /app/chrome/downurl).
var (
	m115N, _ = new(big.Int).SetString(
		"8686980c0f5a24c4b9d43020cd2c22703ff3f450756529058b1cf88f09b86021"+
			"36477198a6e2683149659bd122c33592fdb5ad47944ad1ea4d36c6b172aad633"+
			"8c3bb6ac6227502d010993ac967d1aef00f0c8e038de2e4d3bc2ec368af2e9f1"+
			"0a6f1eda4f7262f136420c07c331b871bf139f74f3010e3c4fe57df3afb71683",
		16,
	)
	m115E      = big.NewInt(0x10001)
	m115KeyLen = m115N.BitLen() / 8

	m115XORKeySeed = []byte{
		0xf0, 0xe5, 0x69, 0xae, 0xbf, 0xdc, 0xbf, 0x8a,
		0x1a, 0x45, 0xe8, 0xbe, 0x7d, 0xa6, 0x73, 0xb8,
		0xde, 0x8f, 0xe7, 0xc4, 0x45, 0xda, 0x86, 0xc4,
		0x9b, 0x64, 0x8b, 0x14, 0x6a, 0xb4, 0xf1, 0xaa,
		0x38, 0x01, 0x35, 0x9e, 0x26, 0x69, 0x2c, 0x86,
		0x00, 0x6b, 0x4f, 0xa5, 0x36, 0x34, 0x62, 0xa6,
		0x2a, 0x96, 0x68, 0x18, 0xf2, 0x4a, 0xfd, 0xbd,
		0x6b, 0x97, 0x8f, 0x4d, 0x8f, 0x89, 0x13, 0xb7,
		0x6c, 0x8e, 0x93, 0xed, 0x0e, 0x0d, 0x48, 0x3e,
		0xd7, 0x2f, 0x88, 0xd8, 0xfe, 0xfe, 0x7e, 0x86,
		0x50, 0x95, 0x4f, 0xd1, 0xeb, 0x83, 0x26, 0x34,
		0xdb, 0x66, 0x7b, 0x9c, 0x7e, 0x9d, 0x7a, 0x81,
		0x32, 0xea, 0xb6, 0x33, 0xde, 0x3a, 0xa9, 0x59,
		0x34, 0x66, 0x3b, 0xaa, 0xba, 0x81, 0x60, 0x48,
		0xb9, 0xd5, 0x81, 0x9c, 0xf8, 0x6c, 0x84, 0x77,
		0xff, 0x54, 0x78, 0x26, 0x5f, 0xbe, 0xe8, 0x1e,
		0x36, 0x9f, 0x34, 0x80, 0x5c, 0x45, 0x2c, 0x9b,
		0x76, 0xd5, 0x1b, 0x8f, 0xcc, 0xc3, 0xb8, 0xf5,
	}

	m115XORClientKey = []byte{
		0x78, 0x06, 0xad, 0x4c, 0x33, 0x86, 0x5d, 0x18,
		0x4c, 0x01, 0x3f, 0x46,
	}
)

// M115Key represents a per-request 16-byte random key used to encrypt/decrypt 115 Pro API payloads.
type M115Key [16]byte

func GenerateM115Key() (M115Key, error) {
	var k M115Key
	_, err := io.ReadFull(rand.Reader, k[:])
	return k, err
}

func m115DeriveKey(seed []byte, size int) []byte {
	key := make([]byte, size)
	for i := 0; i < size; i++ {
		key[i] = (seed[i] + m115XORKeySeed[size*i]) & 0xff
		key[i] ^= m115XORKeySeed[size*(size-i-1)]
	}
	return key
}

func m115XORTransformInPlace(data, key []byte) {
	dataSize, keySize := len(data), len(key)
	mod := dataSize % 4
	if mod > 0 {
		for i := 0; i < mod; i++ {
			data[i] ^= key[i%keySize]
		}
	}
	for i := mod; i < dataSize; i++ {
		data[i] ^= key[(i-mod)%keySize]
	}
}

func m115Reverse(b []byte) {
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
}

func m115RSAEncrypt(input []byte) []byte {
	buf := &bytes.Buffer{}
	for remain := len(input); remain > 0; {
		sliceSize := m115KeyLen - 11
		if sliceSize > remain {
			sliceSize = remain
		}
		padSize := m115KeyLen - sliceSize - 3
		padData := make([]byte, padSize)
		_, _ = rand.Read(padData)

		block := make([]byte, m115KeyLen)
		block[0], block[1] = 0, 2
		for i, b := range padData {
			block[2+i] = b%0xff + 0x01
		}
		block[padSize+2] = 0
		copy(block[padSize+3:], input[:sliceSize])

		ret := new(big.Int).Exp(new(big.Int).SetBytes(block), m115E, m115N).Bytes()
		if fill := m115KeyLen - len(ret); fill > 0 {
			buf.Write(make([]byte, fill))
		}
		buf.Write(ret)

		input = input[sliceSize:]
		remain -= sliceSize
	}
	return buf.Bytes()
}

func m115RSADecrypt(input []byte) []byte {
	buf := &bytes.Buffer{}
	for remain := len(input); remain > 0; {
		sliceSize := m115KeyLen
		if sliceSize > remain {
			sliceSize = remain
		}
		ret := new(big.Int).Exp(new(big.Int).SetBytes(input[:sliceSize]), m115E, m115N).Bytes()
		for i, b := range ret {
			if b == 0 && i != 0 {
				buf.Write(ret[i+1:])
				break
			}
		}
		input = input[sliceSize:]
		remain -= sliceSize
	}
	return buf.Bytes()
}

// M115Encode encrypts plaintext JSON for /app/chrome/downurl using the per-request key.
func M115Encode(plaintext []byte, key M115Key) string {
	buf := make([]byte, 16+len(plaintext))
	copy(buf[:16], key[:])
	copy(buf[16:], plaintext)

	m115XORTransformInPlace(buf[16:], m115DeriveKey(key[:], 4))
	m115Reverse(buf[16:])
	m115XORTransformInPlace(buf[16:], m115XORClientKey)

	return base64.StdEncoding.EncodeToString(m115RSAEncrypt(buf))
}

// M115Decode decrypts base64 response data from /app/chrome/downurl using the request's M115Key.
func M115Decode(encoded string, key M115Key) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	data := m115RSADecrypt(raw)
	if len(data) <= 16 {
		return nil, errors.New("m115 decoded payload too short")
	}
	output := make([]byte, len(data)-16)
	copy(output, data[16:])
	m115XORTransformInPlace(output, m115DeriveKey(data[:16], 12))
	m115Reverse(output)
	m115XORTransformInPlace(output, m115DeriveKey(key[:], 4))
	return output, nil
}
