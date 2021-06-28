package org.apache.knox.ddf;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.Collections;

import java.util.Comparator;

public class Datalake {
  private String datalake;
  private Map<String, Map<String, Object>> datalake_roles;
  private Map<String, Map<String, String>> storage;
  private Map<String, Map<String, Map<String, String>>> permissions;
  private Map<String, List<String>> pathToRolesIndex = new HashMap<String, List<String>>();
  private List<String> pathBestMatchIndex = new ArrayList();
  private Map<String, Integer> roleAndPathToRank = new HashMap<String, Integer>();

  public Datalake() {
  }

  public String getDatalake() {
    return datalake;
  }

  public void setDatalake(String datalake) {
    this.datalake = datalake;
  }

  public Map<String, Map<String, Object>> getDatalake_roles() {
    return datalake_roles;
  }

  public void setDatalake_roles(Map<String, Map<String, Object>> roles) {
    this.datalake_roles = roles;
  }

  public Map<String, Map<String, String>> getStorage() {
    return storage;
  }

  public void setStorage(Map<String, Map<String, String>> storage) {
    this.storage = storage;
  }

  public Map<String, Map<String, Map<String, String>>> getPermissions() {
    return permissions;
  }

  public void setPermissions(Map<String, Map<String, Map<String, String>>> permissions) {
    this.permissions = permissions;
  }

  public void initialize() {
    // index the paths to datalake roles
    for (String name : datalake_roles.keySet()) {
      Map<String, Object> role = datalake_roles.get(name);
      List<String> perms = (List<String>) role.get("permissions");
      for (String perm : perms) {
        String[] elements = perm.split(":");
        if (elements[0].equals("storage")) {
          List<String> roles = pathToRolesIndex.get(getStoragePathByName(elements[2]));
          if (roles == null) {
            roles = new ArrayList<String>();
          }
          pathToRolesIndex.put(getStoragePathByName(elements[2]), roles);
          int rank = getPermissionRank("storage", elements[1]);
          roleAndPathToRank.put(name + "_"  + getStoragePathByName(elements[2]), rank);
          roles.add(name);
        }
      }
    }

    Set<String> paths = pathToRolesIndex.keySet();
    pathBestMatchIndex.addAll(paths);
    pathBestMatchIndex.sort(Comparator.comparingInt(String::length));
    Collections.reverse(pathBestMatchIndex);
  }

  public int getPermissionRank(String permissionType, String permission) {
    Map<String, Map<String, String>> perms = permissions.get(permissionType);
    return Integer.parseInt(perms.get(permission).get("rank"));
  }

  public String getStoragePathByName(String name) {
    Map<String, String> location = this.storage.get(name);
    return location.get("path");
  }

  public List<String> getDatalakeRolesForPath(String path) {
    return pathToRolesIndex.get(path);
  }

  /**
   * Get the list of datalake roles for the best match of the given
   * path to a configured storage path in order by the rank of the
   * permissions binding the path to the role.
   */
  public List<String> getRankedDatalakeRolesForPath(String path) {
    // find longest (most specific) matching path (best match)
    String bestMatch = getBestMatchPath(path);

    // if there are multiple datalake roles sort the list by strength
    // based on the rank of the permission that ties the path to the datalake role
    List<String> roles = null;
    List<String> rankedRoles = null;
    if (bestMatch != null) {
      roles = getDatalakeRolesForPath(bestMatch);
      rankedRoles = (ArrayList<String>) ((ArrayList)roles).clone();

      if (roles.size() > 1) {
        for (String role : roles) {
          int rank = roleAndPathToRank.get(role + "_" + bestMatch);
          rankedRoles.set(rank-1, role);
        }
      }
      else {
        rankedRoles.add(roles.get(0));
      }
    }

    return rankedRoles;
  }

  /**
   * retrieve the best matching storage path based on
   * the assumption that the longest path match is the
   * most specific and therefore best. This is for accommodating
   * subfolders and objects within paths for which there
   * is no explicit storage path defined.
   */
  public String getBestMatchPath(String path) {
    // This implementation assumes that the pathBestMatchIndex is
    // sorted by length of the defined paths.
    String bestMatch = null;
    for (String storagePath : pathBestMatchIndex) {
      if (path.startsWith(storagePath)) {
        bestMatch = storagePath;
        break;
      }
    }
    return bestMatch;
  }
}
