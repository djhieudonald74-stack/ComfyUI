#version 300 es
precision highp float;

uniform sampler2D u_image0;
uniform float u_float0; // temperature (-100 to 100)
uniform float u_float1; // tint (-100 to 100)
uniform float u_float2; // vibrance (-100 to 100)
uniform float u_float3; // saturation (-100 to 100)

in vec2 v_texCoord;
out vec4 fragColor;

void main() {
    vec4 tex = texture(u_image0, v_texCoord);
    vec3 color = tex.rgb;
    
    // Scale inputs: -100/100 → -1/1
    float temperature = u_float0 * 0.01;
    float tint = u_float1 * 0.01;
    float vibrance = u_float2 * 0.01;
    float saturation = u_float3 * 0.01;
    
    // Temperature (warm/cool): positive = warm, negative = cool
    color.r = clamp(color.r + temperature * 0.3, 0.0, 1.0);
    color.b = clamp(color.b - temperature * 0.3, 0.0, 1.0);
    
    // Tint (green/magenta): positive = green, negative = magenta
    color.g = clamp(color.g + tint * 0.3, 0.0, 1.0);
    color.r = clamp(color.r - tint * 0.15, 0.0, 1.0);
    color.b = clamp(color.b - tint * 0.15, 0.0, 1.0);
    
    // Vibrance Pro with skin protection
    if (vibrance != 0.0) {
        float maxC = max(color.r, max(color.g, color.b));
        float minC = min(color.r, min(color.g, color.b));
        float sat = maxC - minC;
        float gray = dot(color, vec3(0.299, 0.587, 0.114));
        
        if (vibrance < 0.0) {
            // Desaturate: -100 → gray
            color = mix(vec3(gray), color, 1.0 + vibrance);
        } else {
            // Boost less saturated colors more
            float vibranceAmt = vibrance * (1.0 - sat);
            
            // Skin tone protection (hardcoded 0.5)
            float skinTone = 0.0;
            if (color.r > color.g && color.g > color.b) {
                float warmth = (color.r - color.b) / max(maxC, 0.001);
                skinTone = warmth * sat * (1.0 - sat);
            }
            vibranceAmt *= (1.0 - skinTone * 0.5);
            
            color = mix(vec3(gray), color, 1.0 + vibranceAmt * 2.0);
        }
    }
    
    // Saturation
    if (saturation != 0.0) {
        float gray = dot(color, vec3(0.299, 0.587, 0.114));
        float satMix = saturation < 0.0
            ? 1.0 + saturation          // -100 → gray
            : 1.0 + saturation * 2.0;   // +100 → 3x boost
        color = mix(vec3(gray), color, satMix);
    }
    
    fragColor = vec4(clamp(color, 0.0, 1.0), tex.a);
}