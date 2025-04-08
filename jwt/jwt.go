package jwt

import (
	"errors"
	"github.com/golang-jwt/jwt/v5"
	"regexp"
	"time"
)

type Config struct {
	TokenExpire        time.Duration `help:"token有效期" devDefault:"24h0m0s" default:"30m0s"`
	RefreshTokenExpire time.Duration `help:"刷新token有效期" default:"720h0m0s"`
	Key                string        `help:"JWT加密key" default:"|^_^|"`
}

type JWT struct {
	Config
}

type TokenPayload struct {
	UserId    int64  `json:"uid"`
	Username  string `json:"une"`
	IsRefresh bool   `json:"irf"`
}

type TokenClaims struct {
	jwt.RegisteredClaims
	TokenPayload
}

func NewJwt(conf Config) *JWT {
	return &JWT{conf}
}

// CreateToken 创建jwt token
func (j *JWT) CreateToken(tp TokenPayload) (string, int64, error) {
	return j.genToken(tp, j.TokenExpire)
}

// CreateRefreshToken 创建jwt token
func (j *JWT) CreateRefreshToken(tp TokenPayload) (string, int64, error) {
	return j.genToken(tp, j.RefreshTokenExpire)
}

func (j *JWT) ValidateToken(tokenString string) (*TokenClaims, error) {
	re := regexp.MustCompile(`(?i)Bearer `)
	tokenString = re.ReplaceAllString(tokenString, "")
	if tokenString == "" {
		return nil, errors.New("token is empty")
	}
	token, err := jwt.ParseWithClaims(tokenString, &TokenClaims{}, func(token *jwt.Token) (interface{}, error) {
		return []byte(j.Key), nil
	})
	if claims, ok := token.Claims.(*TokenClaims); ok && token.Valid {
		return claims, nil
	} else {
		return nil, err
	}
}

func (j *JWT) genToken(tp TokenPayload, t time.Duration) (string, int64, error) {
	exp := time.Now().Add(t)
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, TokenClaims{
		TokenPayload: tp,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(exp),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now()),
			Issuer:    "",
			Subject:   "",
			ID:        "",
			Audience:  []string{},
		},
	})

	// Sign and get the complete encoded token as a string using the key
	tokenString, err := token.SignedString([]byte(j.Key))
	if err != nil {
		return "", 0, err
	}
	return tokenString, exp.Unix(), nil
}
