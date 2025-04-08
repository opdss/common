package jwt_test

import (
	"fmt"
	"testing"
	"time"
	"wapoker/pkg/jwt"
)

func TestJwt(t *testing.T) {
	j := jwt.NewJwt(jwt.Config{
		TokenExpire:        time.Second * 600,
		RefreshTokenExpire: time.Second * 6000,
		Key:                "afweaf",
	})
	s, ts, err := j.CreateToken(jwt.TokenPayload{
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
