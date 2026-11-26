// Copyright 2026, Staffbase SE and contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package llm

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// sameOriginHTTPClient returns the HTTP client the Claude analyzer sends
// requests with. It follows a redirect only to the scheme and host of the
// original request. Go drops Authorization and Cookie on a redirect to another
// domain, but not Anthropic's X-Api-Key, so a gateway at ANTHROPIC_BASE_URL
// that redirects elsewhere would otherwise get the key forwarded there.
// Requiring the same scheme also refuses an HTTPS-to-HTTP downgrade. The
// OpenAI SDK enforces the same rule in its own transport.
func sameOriginHTTPClient() *http.Client {
	return &http.Client{CheckRedirect: checkSameOriginRedirect}
}

func checkSameOriginRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	from := via[0].URL
	if req.URL.Scheme != from.Scheme || !strings.EqualFold(req.URL.Host, from.Host) {
		return fmt.Errorf("refusing redirect from %s://%s to %s://%s: the API key must not leave the configured origin",
			from.Scheme, from.Host, req.URL.Scheme, req.URL.Host)
	}
	return nil
}
