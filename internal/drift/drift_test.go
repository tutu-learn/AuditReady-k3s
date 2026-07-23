package drift

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func testObj() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]interface{}{
			"name":      "cm",
			"namespace": "default",
		},
		"data": map[string]interface{}{"key": "value"},
	}}
}

func TestHashIgnoresServerMetadata(t *testing.T) {
	a := testObj()
	b := testObj()
	b.SetResourceVersion("12345")
	b.SetUID("some-uid")
	b.SetGeneration(7)
	b.SetCreationTimestamp(metav1.NewTime(time.Unix(1700000000, 0)))
	b.Object["metadata"].(map[string]interface{})["managedFields"] = []interface{}{
		map[string]interface{}{"manager": "kubectl", "operation": "Update"},
	}

	if HashObject(a) != HashObject(b) {
		t.Fatalf("hash changed with server-managed metadata:\n a=%s\n b=%s", HashObject(a), HashObject(b))
	}
}

func TestHashDetectsSpecChange(t *testing.T) {
	a := testObj()
	b := testObj()
	b.Object["data"].(map[string]interface{})["key"] = "changed"

	if HashObject(a) == HashObject(b) {
		t.Fatal("hash did not change after content change")
	}
}

func TestStampAndIgnore(t *testing.T) {
	obj := testObj()
	before := HashObject(obj)

	Stamp(obj)
	if obj.GetAnnotations()[HashAnnotation] != before {
		t.Fatalf("stamp %q != pre-stamp hash %q", obj.GetAnnotations()[HashAnnotation], before)
	}
	// HashObject must ignore its own annotation.
	if HashObject(obj) != before {
		t.Fatal("HashObject changed after stamping")
	}
	// Stamping again is idempotent.
	Stamp(obj)
	if obj.GetAnnotations()[HashAnnotation] != before {
		t.Fatal("stamp not idempotent")
	}
}

func TestCheck(t *testing.T) {
	obj := testObj()
	h := HashObject(obj)

	match, diff := Check(obj, h)
	if !match || diff != "" {
		t.Fatalf("expected match, got match=%v diff=%q", match, diff)
	}

	obj.Object["data"].(map[string]interface{})["key"] = "drifted"
	match, diff = Check(obj, h)
	if match {
		t.Fatal("expected mismatch")
	}
	if diff == "" {
		t.Fatal("expected a diff summary")
	}
}

func TestCheckSecretDiffContainsNoData(t *testing.T) {
	secret := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   map[string]interface{}{"name": "s", "namespace": "default"},
		"data":       map[string]interface{}{"password": "c2VjcmV0"},
	}}
	_, diff := Check(secret, "deadbeef")
	if diff == "" {
		t.Fatal("expected a diff summary")
	}
	for _, leak := range []string{"password", "c2VjcmV0", "data"} {
		if strings.Contains(diff, leak) {
			t.Fatalf("diff leaks secret material %q: %s", leak, diff)
		}
	}
}
