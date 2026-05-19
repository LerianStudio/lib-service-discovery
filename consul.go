package libsd

import (
	"context"
	"fmt"

	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/hashicorp/consul/api"
)

type consulRegistry struct {
	client *api.Client
	logger log.Logger
}

func newConsulRegistry(addr string, logger log.Logger) (Registry, error) {
	if logger == nil {
		logger = log.NewNop()
	}

	cfg := api.DefaultConfig()
	cfg.Address = addr

	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("consul: create client: %w", err)
	}

	return &consulRegistry{client: client, logger: logger}, nil
}

func (r *consulRegistry) Register(ctx context.Context, svc Service) error {
	if r == nil {
		return ErrNilRegistry
	}

	reg := &api.AgentServiceRegistration{
		ID:      svc.ID,
		Name:    svc.Name,
		Address: svc.Address,
		Port:    svc.Port,
		Tags:    svc.Tags,
		Meta:    svc.Meta,
	}

	if svc.HealthCheck != nil {
		reg.Check = &api.AgentServiceCheck{
			HTTP:                           svc.HealthCheck.HTTP,
			Interval:                       svc.HealthCheck.Interval,
			Timeout:                        svc.HealthCheck.Timeout,
			DeregisterCriticalServiceAfter: "30s",
		}
	}

	if err := r.client.Agent().ServiceRegister(reg); err != nil {
		return fmt.Errorf("consul: register %s: %w", svc.Name, err)
	}

	r.logger.Log(ctx, log.LevelDebug, "service registered",
		log.String("id", svc.ID),
		log.String("name", svc.Name),
		log.String("addr", svc.Addr()))

	return nil
}

func (r *consulRegistry) Deregister(ctx context.Context, serviceID string) error {
	if r == nil {
		return ErrNilRegistry
	}

	if err := r.client.Agent().ServiceDeregister(serviceID); err != nil {
		return fmt.Errorf("consul: deregister %s: %w", serviceID, err)
	}

	r.logger.Log(ctx, log.LevelDebug, "service deregistered", log.String("id", serviceID))

	return nil
}

func (r *consulRegistry) Resolve(ctx context.Context, name, tag string) (Service, error) {
	if r == nil {
		return Service{}, ErrNilRegistry
	}

	entries, _, err := r.client.Health().Service(name, tag, true, nil)
	if err != nil {
		return Service{}, fmt.Errorf("consul: resolve %s: %w", name, err)
	}

	if len(entries) == 0 {
		return Service{}, fmt.Errorf("%w: %s", ErrNoHealthyInstances, name)
	}

	e := entries[0]

	return Service{
		ID:      e.Service.ID,
		Name:    e.Service.Service,
		Address: e.Service.Address,
		Port:    e.Service.Port,
		Tags:    e.Service.Tags,
		Meta:    e.Service.Meta,
	}, nil
}

func (r *consulRegistry) Watch(ctx context.Context, name string) (<-chan Event, error) {
	if r == nil {
		ch := make(chan Event)
		close(ch)

		return ch, ErrNilRegistry
	}

	ch := make(chan Event, 16)

	go func() {
		defer close(ch)

		var lastIndex uint64

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			entries, meta, err := r.client.Health().Service(name, "", false, &api.QueryOptions{
				WaitIndex: lastIndex,
			})
			if err != nil {
				r.logger.Log(ctx, log.LevelWarn, "consul watch poll failed",
					log.String("service", name),
					log.Err(err))

				continue
			}

			if meta.LastIndex == lastIndex {
				continue
			}

			lastIndex = meta.LastIndex

			for _, e := range entries {
				eventType := EventRegistered

				for _, check := range e.Checks {
					if check.Status == api.HealthCritical {
						eventType = EventDeregistered

						break
					}
				}

				ch <- Event{
					Type: eventType,
					Service: Service{
						ID:      e.Service.ID,
						Name:    e.Service.Service,
						Address: e.Service.Address,
						Port:    e.Service.Port,
						Tags:    e.Service.Tags,
						Meta:    e.Service.Meta,
					},
				}
			}
		}
	}()

	return ch, nil
}
