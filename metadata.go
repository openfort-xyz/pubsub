package pubsub

// Metadata represents the key-value pairs of an event.
type Metadata map[string]interface{}

// NewMetadata creates a new metadata.
func NewMetadata() Metadata {
	return make(Metadata)
}

// Get returns the value associated with the key.
func (m Metadata) Get(key string) (interface{}, bool) {
	v, ok := m[key]
	return v, ok
}

// Set sets the key-value pair in the metadata.
func (m Metadata) Set(key string, value interface{}) {
	m[key] = value
}

// Del deletes the key-value pair from the metadata.
func (m Metadata) Del(key string) {
	delete(m, key)
}

// Keys returns the keys of the metadata.
func (m Metadata) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
