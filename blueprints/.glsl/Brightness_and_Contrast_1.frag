#version 300 es
precision highp float;

uniform sampler2D u_image0;
uniform float u_float0; // Brightness slider -100..100
uniform float u_float1; // Contrast slider -100..100

in vec2 v_texCoord;
out vec4 fragColor;

const float EPSILON = 1e-5;

vec3 srgbToLinear(vec3 c) {
    return pow(c, vec3(2.2));
}

vec3 linearToSrgb(vec3 c) {
    return pow(c, vec3(1.0/2.2));
}

float mapBrightness(float b) {
    return clamp(b / 100.0, -1.0, 1.0);
}

float mapContrast(float c) {
    return clamp(c / 100.0 + 1.0, 0.0, 2.0);
}

void main() {
    vec4 orig = texture(u_image0, v_texCoord);

    float brightness = mapBrightness(u_float0);
    float contrast   = mapContrast(u_float1);

    // Early exit if no adjustment
    if (abs(brightness) < EPSILON && abs(contrast - 1.0) < EPSILON) {
        fragColor = orig;
        return;
    }

    vec3 lin = srgbToLinear(orig.rgb);

    lin = (lin - 0.5) * contrast + brightness + 0.5;

    // Convert back to sRGB
    vec3 result = linearToSrgb(clamp(lin, 0.0, 1.0));

    fragColor = vec4(result, orig.a);
}
