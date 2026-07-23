// Package drift detects out-of-band changes to managed objects by hashing
// the last applied state and comparing it with the live object.
package drift

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// HashAnnotation records the hash of the last applied object state.
const HashAnnotation = "k8s-agent.io/last-applied-hash"

// metadata keys dropped before hashing: they change on every write and carry
// no intent.
var volatileMetaKeys = []string{
	"resourceVersion", "uid", "generation", "creationTimestamp", "managedFields",
}

// HashObject returns the hex sha256 of the object's meaningful content:
// server-managed metadata and the hash annotation itself are stripped first.
// encoding/json sorts map keys, so the result is deterministic.
func HashObject(obj *unstructured.Unstructured) string {
	cpy := obj.DeepCopy()
	m := cpy.Object

	if meta, ok := m["metadata"].(map[string]interface{}); ok {
		for _, k := range volatileMetaKeys {
			delete(meta, k)
		}
		if ann, ok := meta["annotations"].(map[string]interface{}); ok {
			delete(ann, HashAnnotation)
			if len(ann) == 0 {
				delete(meta, "annotations")
			}
		}
	}

	raw, err := json.Marshal(m)
	if err != nil {
		// Unstructured maps are JSON-shaped by construction; a marshal error
		// means corrupt input. Hash what we can rather than panic.
		raw = []byte(fmt.Sprintf("%v", m))
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Check compares the live object's hash with expectedHash. On mismatch it
// returns a compact human summary naming the resource and both hashes.
// Secret data is never included in the diff.
func Check(live *unstructured.Unstructured, expectedHash string) (match bool, diff string) {
	liveHash := HashObject(live)
	if liveHash == expectedHash {
		return true, ""
	}
	return false, fmt.Sprintf("drift detected on %s %s/%s: expected hash %s, live hash %s",
		live.GetKind(), live.GetNamespace(), live.GetName(), shortHash(expectedHash), shortHash(liveHash))
}

// Stamp sets HashAnnotation on obj to the hash of its current content.
func Stamp(obj *unstructured.Unstructured) {
	ann := obj.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	ann[HashAnnotation] = HashObject(obj)
	obj.SetAnnotations(ann)
}

func shortHash(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}
