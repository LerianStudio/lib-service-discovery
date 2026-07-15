//go:build unit

package libsd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEndpoint_Addr_HostPort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint Endpoint
		want     string
	}{
		{
			name:     "normal port",
			endpoint: Endpoint{Address: "fees.internal", Port: 8080},
			want:     "fees.internal:8080",
		},
		{
			name:     "port zero renders as host:0",
			endpoint: Endpoint{Address: "fees.internal", Port: 0},
			want:     "fees.internal:0",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.endpoint.Addr())
		})
	}
}

// TestService_InternalField_AcceptsEndpointPointer is a compile-level assertion
// that Service carries an *Endpoint Internal field and that the EndpointView
// constants External and Internal exist with the expected string values.
func TestService_InternalField_AcceptsEndpointPointer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		view EndpointView
		want string
	}{
		{name: "external is the default view", view: External, want: "external"},
		{name: "internal is the cluster view", view: Internal, want: "internal"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, string(tt.view))
		})
	}

	ep := &Endpoint{Address: "fees.svc.cluster.local", Port: 4002, Scheme: "http"}
	svc := Service{Internal: ep}
	assert.Same(t, ep, svc.Internal)
	assert.Equal(t, "fees.svc.cluster.local:4002", svc.Internal.Addr())
}

// TestService_EndpointFor_SelectsView asserts the asymmetric view-selection
// semantics of Service.EndpointFor, which reads the endpoint POINTERS directly
// (never synthesizing a view from the deprecated flat fields) and returns
// (Endpoint, error):
//   - External (and any unknown value): returns *s.External when set, else
//     ErrEndpointViewUnavailable — it NEVER degrades to Internal and NEVER derives
//     an external view from the flat mirror.
//   - Internal: returns *s.Internal when set; otherwise degrades to *s.External
//     WITHOUT error (the warning is logged by the caller, not here); when neither
//     exists, ErrEndpointViewUnavailable.
func TestService_EndpointFor_SelectsView(t *testing.T) {
	t.Parallel()

	externalEP := &Endpoint{Address: "fees.dev.example.net", Port: 443, Scheme: "https"}
	external := Service{External: externalEP}
	internalEP := &Endpoint{Address: "fees.svc.cluster.local", Port: 4002, Scheme: "http"}
	both := Service{External: externalEP, Internal: internalEP}
	internalOnly := Service{Internal: internalEP}

	tests := []struct {
		name    string
		svc     Service
		view    EndpointView
		want    Endpoint
		wantErr error
	}{
		{
			name: "external view with internal nil returns external endpoint",
			svc:  external,
			view: External,
			want: Endpoint{Address: "fees.dev.example.net", Port: 443, Scheme: "https"},
		},
		{
			name: "external view with internal set ignores internal endpoint",
			svc:  both,
			view: External,
			want: Endpoint{Address: "fees.dev.example.net", Port: 443, Scheme: "https"},
		},
		{
			name:    "external view against internal-only provider is unavailable",
			svc:     internalOnly,
			view:    External,
			wantErr: ErrEndpointViewUnavailable,
		},
		{
			name: "internal view with internal set returns internal endpoint",
			svc:  both,
			view: Internal,
			want: Endpoint{Address: "fees.svc.cluster.local", Port: 4002, Scheme: "http"},
		},
		{
			name: "internal view with internal nil degrades to external without error",
			svc:  external,
			view: Internal,
			want: Endpoint{Address: "fees.dev.example.net", Port: 443, Scheme: "https"},
		},
		{
			name:    "both endpoints nil is unavailable for any view",
			svc:     Service{},
			view:    Internal,
			wantErr: ErrEndpointViewUnavailable,
		},
		{
			name: "unknown view value behaves like external",
			svc:  external,
			view: EndpointView("bogus"),
			want: Endpoint{Address: "fees.dev.example.net", Port: 443, Scheme: "https"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.svc.EndpointFor(tt.view)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				assert.Equal(t, Endpoint{}, got)

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestService_externalEndpoint_PurePriority asserts the pure helper: External
// pointer wins; otherwise any non-zero flat field synthesizes an endpoint;
// otherwise nil.
func TestService_externalEndpoint_PurePriority(t *testing.T) {
	t.Parallel()

	ext := &Endpoint{Address: "ingress", Port: 443, Scheme: "https"}

	assert.Same(t, ext, Service{External: ext}.externalEndpoint(),
		"External pointer must be returned as-is")

	got := Service{Address: "flat", Port: 80, Scheme: "http"}.externalEndpoint()
	if assert.NotNil(t, got) {
		assert.Equal(t, Endpoint{Address: "flat", Port: 80, Scheme: "http"}, *got)
	}

	assert.NotNil(t, Service{Scheme: "https"}.externalEndpoint(),
		"a lone non-zero flat field still synthesizes an endpoint")

	assert.Nil(t, Service{}.externalEndpoint(),
		"no External and all-zero flat fields -> nil")
}

// TestService_normalizeEndpoints_MirrorsBothDirections asserts that normalize
// promotes flat fields into External when External is nil, and mirrors External
// back into the flat shim when External is set.
func TestService_normalizeEndpoints_MirrorsBothDirections(t *testing.T) {
	t.Parallel()

	// flat -> External
	s := Service{Address: "flat.example", Port: 8080, Scheme: "http"}
	s.normalizeEndpoints()
	if assert.NotNil(t, s.External) {
		assert.Equal(t, Endpoint{Address: "flat.example", Port: 8080, Scheme: "http"}, *s.External)
	}

	// External -> flat
	s2 := Service{External: &Endpoint{Address: "ext.example", Port: 443, Scheme: "https"}}
	s2.normalizeEndpoints()
	assert.Equal(t, "ext.example", s2.Address)
	assert.Equal(t, 443, s2.Port)
	assert.Equal(t, "https", s2.Scheme)

	// all-zero stays nil
	s3 := Service{}
	s3.normalizeEndpoints()
	assert.Nil(t, s3.External)
}
