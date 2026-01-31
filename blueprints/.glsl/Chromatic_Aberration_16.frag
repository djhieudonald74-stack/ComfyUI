#version 300 es
precision highp float;

uniform sampler2D u_image0;
uniform int u_int0;      // Mode
uniform float u_float0;  // Amount (0 to 100)

in vec2 v_texCoord;
out vec4 fragColor;

const int MODE_LINEAR   = 0;
const int MODE_RADIAL   = 1;
const int MODE_BARREL   = 2;
const int MODE_SWIRL    = 3;
const int MODE_DIAGONAL = 4;

void main() {
    vec2 uv = v_texCoord;
    vec4 original = texture(u_image0, uv);
    
    float amount = u_float0 * 0.0005;
    
    if (amount == 0.0) {
        fragColor = original;
        return;
    }
    
    vec2 centered = uv - 0.5;
    float r = length(centered);
    vec2 dir = normalize(centered + 0.001);
    vec2 offset;
    
    if (u_int0 == MODE_LINEAR) {
        // Horizontal shift
        offset = vec2(amount, 0.0);
    }
    else if (u_int0 == MODE_RADIAL) {
        // Outward from center, stronger at edges
        offset = dir * r * amount * 4.0;
    }
    else if (u_int0 == MODE_BARREL) {
        // Lens distortion simulation (r² falloff)
        offset = dir * r * r * amount * 8.0;
    }
    else if (u_int0 == MODE_SWIRL) {
        // Perpendicular to radial (rotational aberration)
        vec2 perp = vec2(-dir.y, dir.x);
        offset = perp * r * amount * 4.0;
    }
    else if (u_int0 == MODE_DIAGONAL) {
        // 45° offset
        offset = vec2(amount, amount) * 0.707;
    }
    
    float red = texture(u_image0, uv + offset).r;
    float green = original.g;
    float blue = texture(u_image0, uv - offset).b;
    
    fragColor = vec4(red, green, blue, original.a);
}