package service

import (
	"context"
	"database/sql"

	"github.com/spike/goTogether/pkg/auth"
	userpb "github.com/spike/goTogether/proto/user"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UserService struct {
	userpb.UnimplementedUserServiceServer
	db *sql.DB
}

func NewUserService(db *sql.DB) *UserService {
	return &UserService{db: db}
}

func (s *UserService) Register(ctx context.Context, req *userpb.RegisterRequest) (*userpb.RegisterResponse, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "hash password: %v", err)
	}

	var userID int64
	err = s.db.QueryRowContext(ctx,
		"INSERT INTO users (username, email, password_hash) VALUES ($1, $2, $3) RETURNING id",
		req.Username, req.Email, string(hash),
	).Scan(&userID)
	if err != nil {
		return nil, status.Errorf(codes.AlreadyExists, "email already registered")
	}

	token, err := auth.GenerateToken(userID, req.Username)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "generate token: %v", err)
	}
	return &userpb.RegisterResponse{UserId: userID, Token: token}, nil
}

func (s *UserService) Login(ctx context.Context, req *userpb.LoginRequest) (*userpb.LoginResponse, error) {
	var userID int64
	var username, hash string
	err := s.db.QueryRowContext(ctx,
		"SELECT id, username, password_hash FROM users WHERE email = $1", req.Email,
	).Scan(&userID, &username, &hash)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "user not found")
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(req.Password)); err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "wrong password")
	}

	token, err := auth.GenerateToken(userID, username)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "generate token: %v", err)
	}
	return &userpb.LoginResponse{UserId: userID, Token: token, Username: username}, nil
}

func (s *UserService) GetUser(ctx context.Context, req *userpb.GetUserRequest) (*userpb.UserInfo, error) {
	var info userpb.UserInfo
	var createdAt sql.NullTime
	err := s.db.QueryRowContext(ctx,
		"SELECT id, username, email, avatar_url, created_at FROM users WHERE id = $1", req.UserId,
	).Scan(&info.UserId, &info.Username, &info.Email, &info.AvatarUrl, &createdAt)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "user not found")
	}
	if createdAt.Valid {
		info.CreatedAt = createdAt.Time.Format("2006-01-02T15:04:05Z")
	}
	return &info, nil
}

func (s *UserService) UpdateUser(ctx context.Context, req *userpb.UpdateUserRequest) (*userpb.UserInfo, error) {
	_, err := s.db.ExecContext(ctx,
		"UPDATE users SET username = $1, avatar_url = $2 WHERE id = $3",
		req.Username, req.AvatarUrl, req.UserId,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "update user: %v", err)
	}
	return s.GetUser(ctx, &userpb.GetUserRequest{UserId: req.UserId})
}
