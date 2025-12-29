package project;

import java.security.Permission;

class SystemUtil {
    private static final String PERMISSION_PREFIX = "exitVM.";

    static class ExitTrappedException extends SecurityException {
        private final int status;

        ExitTrappedException(int status) {
            super("Trapped System.exit(" + status + ")");
            this.status = status;
        }

        int getStatus() {
            return status;
        }
    }

    static void forbidSystemExitCall() {
        final SecurityManager securityManager = new SecurityManager() {
            public void checkPermission(Permission permission) {
                String permissionName = permission.getName();
                if (permissionName.startsWith(PERMISSION_PREFIX)) {
                    String suffix = permissionName.substring(PERMISSION_PREFIX.length());
                    int status = Integer.parseInt(suffix);
                    throw new ExitTrappedException(status);
                }
            }
        };
        System.setSecurityManager(securityManager);
    }

    static void allowSystemExitCall() {
        System.setSecurityManager(null);
    }
}
