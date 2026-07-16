package config

import (
	"bytes"
	"testing"
)

func TestEncryptDecrypt(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	pt := []byte("my-secret-key")
	enc, err := Encrypt(pt, key)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := Decrypt(enc, key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pt, dec) {
		t.Fatal("mismatch")
	}
}

func TestDecryptWrongKey(t *testing.T) {
	enc, _ := Encrypt([]byte("secret"), []byte("0123456789abcdef0123456789abcdef"))
	_, err := Decrypt(enc, []byte("ffffffffffffffffffffffffffffffff"))
	if err == nil {
		t.Fatal("expected error")
	}
}
