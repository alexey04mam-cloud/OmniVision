/* Omni-Vision Service Worker — Push Notifications */

self.addEventListener("install", function(e) {
  self.skipWaiting();
});

self.addEventListener("activate", function(e) {
  e.waitUntil(self.clients.claim());
});

self.addEventListener("push", function(e) {
  var data = {title: "Omni-Vision", body: "Новий сигнал!", icon: "/favicon.ico", tag: "omni"};
  try {
    data = e.data.json();
  } catch(err) {
    try { data.body = e.data.text(); } catch(e2) {}
  }
  var options = {
    body: data.body || "",
    icon: data.icon || "/favicon.ico",
    badge: data.badge || "/favicon.ico",
    tag: data.tag || "omni-" + Date.now(),
    data: {url: data.url || "/"},
    vibrate: [200, 100, 200],
    actions: data.actions || []
  };
  e.waitUntil(self.registration.showNotification(data.title || "Omni-Vision", options));
});

self.addEventListener("notificationclick", function(e) {
  e.notification.close();
  var url = (e.notification.data && e.notification.data.url) || "/";
  e.waitUntil(
    self.clients.matchAll({type: "window"}).then(function(clients) {
      for (var i = 0; i < clients.length; i++) {
        if (clients[i].url.indexOf(url) !== -1 && "focus" in clients[i]) {
          return clients[i].focus();
        }
      }
      return self.clients.openWindow(url);
    })
  );
});
