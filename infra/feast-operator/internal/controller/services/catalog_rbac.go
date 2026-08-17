/*
Copyright 2024 Feast Community.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package services

import (
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	dataregistryAPIGroup       = "dataregistry.opendatahub.io"
	dataregistryViewRoleName   = "dataregistry-view"
	dataregistryEditRoleName   = "dataregistry-edit"
	dataregistryAdminRoleName  = "dataregistry-admin"
	authProxyRoleName          = "feast-catalog-auth-proxy"
)

var dataregistryResources = []string{"namespaces", "tables", "volumes"}

func (feast *FeastServices) ReconcileCatalogClusterRoles() error {
	if !feast.IsCatalogMode() {
		return nil
	}

	logger := log.FromContext(feast.Handler.Context)

	apiGroup := dataregistryAPIGroup
	resources := dataregistryResources

	clusterRoles := []struct {
		name   string
		verbs  []string
		labels map[string]string
	}{
		{
			name:  dataregistryViewRoleName,
			verbs: []string{"get", "list"},
			labels: map[string]string{
				"rbac.authorization.k8s.io/aggregate-to-view":  "true",
				"rbac.authorization.k8s.io/aggregate-to-edit":  "true",
				"rbac.authorization.k8s.io/aggregate-to-admin": "true",
			},
		},
		{
			name:  dataregistryEditRoleName,
			verbs: []string{"get", "list", "create", "update", "delete"},
			labels: map[string]string{
				"rbac.authorization.k8s.io/aggregate-to-edit":  "true",
				"rbac.authorization.k8s.io/aggregate-to-admin": "true",
			},
		},
		{
			name:  dataregistryAdminRoleName,
			verbs: []string{"get", "list", "create", "update", "delete"},
			labels: map[string]string{
				"rbac.authorization.k8s.io/aggregate-to-admin": "true",
			},
		},
	}

	for _, role := range clusterRoles {
		desired := &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{
				Name:   role.name,
				Labels: role.labels,
			},
			Rules: []rbacv1.PolicyRule{
				{
					APIGroups: []string{apiGroup},
					Resources: resources,
					Verbs:     role.verbs,
				},
			},
		}

		existing := &rbacv1.ClusterRole{}
		err := feast.Handler.Client.Get(feast.Handler.Context, client.ObjectKeyFromObject(desired), existing)
		if err != nil {
			if apierrors.IsNotFound(err) {
				logger.Info("Creating ClusterRole", "name", role.name)
				if err := feast.Handler.Client.Create(feast.Handler.Context, desired); err != nil {
					return err
				}
				continue
			}
			return err
		}

		existing.Labels = role.labels
		existing.Rules = desired.Rules
		if err := feast.Handler.Client.Update(feast.Handler.Context, existing); err != nil {
			return err
		}
	}

	// Auth proxy ClusterRole for kube-rbac-proxy to perform token reviews and subject access reviews
	authProxyRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: authProxyRoleName,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"authentication.k8s.io"},
				Resources: []string{"tokenreviews"},
				Verbs:     []string{"create"},
			},
			{
				APIGroups: []string{"authorization.k8s.io"},
				Resources: []string{"subjectaccessreviews"},
				Verbs:     []string{"create"},
			},
		},
	}

	existing := &rbacv1.ClusterRole{}
	err := feast.Handler.Client.Get(feast.Handler.Context, client.ObjectKeyFromObject(authProxyRole), existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Creating ClusterRole", "name", authProxyRoleName)
			if err := feast.Handler.Client.Create(feast.Handler.Context, authProxyRole); err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		existing.Rules = authProxyRole.Rules
		if err := feast.Handler.Client.Update(feast.Handler.Context, existing); err != nil {
			return err
		}
	}

	sa := feast.initFeastSA()
	authProxyBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: authProxyRoleName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      sa.Name,
				Namespace: feast.Handler.FeatureStore.Namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     authProxyRoleName,
		},
	}

	existingBinding := &rbacv1.ClusterRoleBinding{}
	err = feast.Handler.Client.Get(feast.Handler.Context, client.ObjectKeyFromObject(authProxyBinding), existingBinding)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Creating ClusterRoleBinding", "name", authProxyRoleName)
			return feast.Handler.Client.Create(feast.Handler.Context, authProxyBinding)
		}
		return err
	}

	existingBinding.Subjects = authProxyBinding.Subjects
	existingBinding.RoleRef = authProxyBinding.RoleRef
	return feast.Handler.Client.Update(feast.Handler.Context, existingBinding)
}
