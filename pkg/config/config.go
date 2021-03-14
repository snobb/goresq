package config

// Config is a configuration for the Resque
type Redis struct {
	Namespace      string
	URI            string `json:"uri"`
	DB             int    `json:"db"`
	MaxIdle        int    `json:"max_idle"`
	MaxActive      int    `json:"max_active"`
	IdleTimeout    int    `json:"idle_timeout"`
	ConnectTimeout int    `json:"connection_timeout"`
}

func (r *Redis) Defaults() {
	if r.MaxIdle == 0 {
		r.MaxIdle = 500
	}

	if r.MaxActive == 0 {
		r.MaxActive = 500
	}

	if r.IdleTimeout == 0 {
		r.IdleTimeout = 5
	}

	if r.ConnectTimeout == 0 {
		r.ConnectTimeout = 0
	}
}
