package service

import (
	"context"

	"github.com/spike/goTogether/collab-service/hub"
	collabpb "github.com/spike/goTogether/proto/collab"
)

type CollabService struct {
	collabpb.UnimplementedCollabServiceServer
	hub *hub.Hub
}

func NewCollabService(h *hub.Hub) *CollabService {
	return &CollabService{hub: h}
}

func (s *CollabService) GetDocState(ctx context.Context, req *collabpb.GetDocStateRequest) (*collabpb.DocState, error) {
	return &collabpb.DocState{
		DocId:   req.DocId,
		Version: 0,
	}, nil
}

func (s *CollabService) GetCollaborators(ctx context.Context, req *collabpb.GetCollaboratorsRequest) (*collabpb.CollaboratorsResponse, error) {
	users := s.hub.GetCollaborators(req.DocId)
	var collaborators []*collabpb.Collaborator
	colors := []string{"#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4", "#FFEAA7", "#DDA0DD"}
	for i, u := range users {
		collaborators = append(collaborators, &collabpb.Collaborator{
			UserId:   u.UserID,
			Username: u.Username,
			Color:    colors[i%len(colors)],
		})
	}
	return &collabpb.CollaboratorsResponse{Collaborators: collaborators}, nil
}
