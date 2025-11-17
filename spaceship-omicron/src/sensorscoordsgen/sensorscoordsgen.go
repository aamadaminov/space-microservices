package main

import (
	"context"
	"log"
	"math/rand/v2"
	"net"
	pb "sensorscoordsgen/sensorproto"
	"time"

	"google.golang.org/grpc"
)

type sensorServiceServer struct {
	pb.UnimplementedSensorServiceServer
}

func (s *sensorServiceServer) GetSensor(ctx context.Context, req *pb.SensorRequest) (*pb.SensorResponse, error) {
	log.Printf("Request coords")
	return &pb.SensorResponse{X: rand.Float64(), Y: rand.Float64(), Z: rand.Float64(), T: time.Now().Format("2006-01-02 15:04:05")}, nil
}

func main() {
	listener, err := net.Listen("tcp", ":50070")
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterSensorServiceServer(grpcServer, &sensorServiceServer{})
	log.Println("gRPC server started on port 50070")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
