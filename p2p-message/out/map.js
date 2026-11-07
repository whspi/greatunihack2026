export default class MapContext {
    #data;
    #width;
    #height;
    #viewDistance;
    get width() { return this.#width; }
    get height() { return this.#height; }
    constructor(width, height, viewDistance) {
        this.#data = new Uint8Array(width * height);
        this.#width = width;
        this.#height = height;
        this.#viewDistance = viewDistance;
    }
    getVisited(x, y) {
        return this.#data[y * this.#width + x] === 1;
    }
    setVisited(cx, cy) {
        const buf = this.#data;
        const w = this.#width;
        const h = this.#height;
        const r = this.#viewDistance;
        const r2 = r * r;
        const y0 = Math.max(0, cy - r);
        const y1 = Math.min(h - 1, cy + r);
        for (let y = y0; y <= y1; y++) {
            const dy = y - cy;
            const dx = Math.floor(Math.sqrt(r2 - dy * dy));
            const left = Math.max(0, cx - dx);
            const right = Math.min(w - 1, cx + dx);
            const row = y * w;
            buf.fill(1, row + left, row + right + 1);
        }
    }
    toString() {
        return Array.from(this.#data, (v, i) => {
            let s = v === 1 ? "#" : " ";
            if (i % this.#width === 0)
                s += "\r\n";
            return s;
        }).join("");
    }
}
