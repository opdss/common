package http

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type Config struct {
	Endpoint string `help:"访问地址" default:"http://localhost:8989"`
	Address  string `help:"监听地址" default:"0.0.0.0:8989"`
}

type Server struct {
	*gin.Engine
	httpSrv *http.Server
	logger  *zap.Logger
	config  Config
}
type Option func(s *Server)

func NewServer(engine *gin.Engine, logger *zap.Logger, conf Config) *Server {
	s := &Server{
		Engine: engine,
		logger: logger,
		config: conf,
	}
	return s
}

func (s *Server) Start(ctx context.Context) error {
	s.httpSrv = &http.Server{
		Addr:    s.config.Address,
		Handler: s,
	}
	s.logger.Sugar().Infof("http server start: %s; endpoint: %s", s.config.Address, s.config.Endpoint)
	if err := s.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.logger.Sugar().Fatalf("listen: %s\n", err)
	}
	return nil
}
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Sugar().Info("Shutting down server...")

	// The context is used to inform the server it has 5 seconds to finish
	// the request it is currently handling
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.httpSrv.Shutdown(ctx); err != nil {
		s.logger.Sugar().Fatal("Server forced to shutdown: ", err)
	}

	s.logger.Sugar().Info("Server exiting")
	return nil
}
