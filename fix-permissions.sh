#!/bin/bash

echo "🔧 修复Docker Compose权限问题"
echo "================================"

# 检查docker-compose位置
DOCKER_COMPOSE_PATHS=(
    "/usr/local/bin/docker-compose"
    "/usr/bin/docker-compose"
    "/bin/docker-compose"
)

echo "🔍 查找docker-compose安装位置..."
for path in "${DOCKER_COMPOSE_PATHS[@]}"; do
    if [ -f "$path" ]; then
        echo "找到: $path"
        
        # 检查权限
        if [ ! -x "$path" ]; then
            echo "❌ $path 没有执行权限"
            echo "🔧 尝试修复权限..."
            
            # 尝试修复权限
            if sudo chmod +x "$path"; then
                echo "✅ 权限修复成功"
            else
                echo "❌ 权限修复失败，请手动执行: sudo chmod +x $path"
            fi
        else
            echo "✅ $path 权限正常"
        fi
    fi
done

# 检查docker命令是否可用
echo ""
echo "🔍 检查Docker服务状态..."
if systemctl is-active --quiet docker; then
    echo "✅ Docker服务运行正常"
else
    echo "❌ Docker服务未运行"
    echo "🔧 尝试启动Docker服务..."
    sudo systemctl start docker
    sudo systemctl enable docker
fi

# 检查当前用户是否在docker组
echo ""
echo "🔍 检查用户权限..."
if groups $USER | grep -q docker; then
    echo "✅ 用户 $USER 已在docker组中"
else
    echo "❌ 用户 $USER 不在docker组中"
    echo "🔧 添加用户到docker组..."
    sudo usermod -aG docker $USER
    echo "⚠️  请注销并重新登录以使组权限生效"
    echo "或者运行: newgrp docker"
fi

# 测试docker-compose
echo ""
echo "🧪 测试docker-compose命令..."
if docker-compose --version; then
    echo "✅ docker-compose 工作正常"
else
    echo "❌ docker-compose 仍有问题"
    
    # 尝试使用docker compose (新版本)
    echo "🔧 尝试使用 'docker compose' (Docker Compose V2)..."
    if docker compose version; then
        echo "✅ 建议使用 'docker compose' 而不是 'docker-compose'"
        echo "💡 可以创建别名: alias docker-compose='docker compose'"
    fi
fi

echo ""
echo "🎯 修复完成！如果问题仍然存在，请："
echo "1. 重新登录系统"
echo "2. 检查Docker是否正确安装"
echo "3. 运行: sudo docker-compose --version"