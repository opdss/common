package jwt

import (
	"fmt"
	"testing"
	"time"
)

func TestJwt(t *testing.T) {
	j := NewJwt(Config{
		TokenExpire:        time.Second * 600,
		RefreshTokenExpire: time.Second * 6000,
		Key:                "afweaf",
	})
	s, ts, err := j.CreateToken(TokenPayload{
		UserId:   23,
		Username: "fwafwef",
	})
	if err != nil {
		t.Fatal(err)
	}

	tp, err := j.ValidateToken(s)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(tp.Username, time.Unix(ts, 0))

}
