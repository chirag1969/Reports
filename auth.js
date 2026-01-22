const AUTH_KEY = "advtus-auth";
const USER_KEY = "advtus-user";
const USERS_URL = "users.json";

function isAuthenticated() {
  return sessionStorage.getItem(AUTH_KEY) === "true";
}

function requireAuth() {
  if (!isAuthenticated()) {
    window.location.href = "login.html";
  }
}

function logout() {
  sessionStorage.removeItem(AUTH_KEY);
  sessionStorage.removeItem(USER_KEY);
  window.location.href = "login.html";
}

async function hashPassword(password, salt) {
  const encoder = new TextEncoder();
  const data = encoder.encode(`${salt}${password}`);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(digest))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

async function authenticateUser(email, password) {
  const response = await fetch(USERS_URL, { cache: "no-store" });
  if (!response.ok) {
    return { success: false, message: "Unable to load user list." };
  }
  const payload = await response.json();
  const users = Array.isArray(payload.users) ? payload.users : [];
  const match = users.find(
    (user) => user.email.toLowerCase() === email.toLowerCase()
  );
  if (!match) {
    return { success: false, message: "Invalid email or password." };
  }
  const computedHash = await hashPassword(password, match.salt);
  if (computedHash !== match.passwordHash) {
    return { success: false, message: "Invalid email or password." };
  }
  sessionStorage.setItem(AUTH_KEY, "true");
  sessionStorage.setItem(USER_KEY, match.email);
  return { success: true };
}

window.advtAuth = {
  authenticateUser,
  isAuthenticated,
  requireAuth,
  logout,
};
