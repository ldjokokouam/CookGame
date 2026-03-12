# 🍳 CookQuiz — Multijoueur temps réel (TypeScript)

## Lancement en 3 commandes

```bash
# Pré-requis : Node.js 18+ et tsx installé globalement
npm install -g tsx

# Place server.ts et client.html dans le même dossier, puis :
tsx server.ts

# Ouvre http://localhost:3000 dans ton navigateur
```

## Structure des fichiers

```
cookquiz/
├── server.ts     ← serveur WebSocket Node.js (TypeScript)
├── client.html   ← frontend servi automatiquement par le serveur
├── tsconfig.json ← config TypeScript strict
└── README.md
```

## Vérification des types

```bash
# Installer @types/node si besoin
npm install -D typescript @types/node

# Vérifier sans compiler
npx tsc --noEmit
```

## Comment jouer

1. Un joueur crée une salle → reçoit un code à 4 lettres (ex: `XKQZ`)
2. Les autres rejoignent sur `http://[IP]:3000` et entrent le code
3. L'hôte démarre quand tout le monde est prêt
4. Tout le monde joue **en même temps**, chrono de 25 secondes
5. Classement après chaque plat — podium final après 7 plats !

## Sur réseau local

```bash
# Trouve ton IP (macOS/Linux)
ifconfig | grep "inet 192"
# Windows
ipconfig

tsx server.ts
# Amis → http://192.168.x.x:3000
```

## Système de points

| Résultat               | Points                        |
|------------------------|-------------------------------|
| ✅ Parfait (0 erreur)  | 300 pts + bonus rapidité (+100 max) |
| 👏 Presque (1 oublié)  | 150 pts + bonus rapidité partiel    |
| 😅 Approximatif        | 50 pts                        |
| 😬 Raté                | 0 pts                         |

## Stack technique

- **Zéro dépendance runtime** — WebSocket RFC 6455 natif (`node:http` + `node:crypto`)
- **TypeScript strict** — types complets : `Dish`, `Player`, `Room`, `Answer`, `ClientMessage`, `WsSocket`…
- **30 plats** : 15 🇫🇷 français + 15 🇪🇸 espagnols
